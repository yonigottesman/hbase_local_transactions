/**
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.master.handler;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableNotDisabledException;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.catalog.CatalogTracker;
import org.apache.hadoop.hbase.catalog.MetaReader;
import org.apache.hadoop.hbase.executor.EventHandler;
import org.apache.hadoop.hbase.executor.EventType;
import org.apache.hadoop.hbase.master.AssignmentManager;
import org.apache.hadoop.hbase.master.BulkAssigner;
import org.apache.hadoop.hbase.master.GeneralBulkAssigner;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.master.MasterCoprocessorHost;
import org.apache.hadoop.hbase.master.RegionStates;
import org.apache.hadoop.hbase.master.ServerManager;
import org.apache.hadoop.hbase.master.TableLockManager;
import org.apache.hadoop.hbase.master.TableLockManager.TableLock;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.zookeeper.KeeperException;
import org.cloudera.htrace.Trace;

/**
 * Handler to run enable of a table.
 */
@InterfaceAudience.Private
public class EnableTableHandler extends EventHandler {
  private static final Log LOG = LogFactory.getLog(EnableTableHandler.class);
  private final TableName tableName;
  private final AssignmentManager assignmentManager;
  private final TableLockManager tableLockManager;
  private final CatalogTracker catalogTracker;
  private boolean skipTableStateCheck = false;
  private TableLock tableLock;

  public EnableTableHandler(Server server, TableName tableName,
      CatalogTracker catalogTracker, AssignmentManager assignmentManager,
      TableLockManager tableLockManager, boolean skipTableStateCheck) {
    super(server, EventType.C_M_ENABLE_TABLE);
    this.tableName = tableName;
    this.catalogTracker = catalogTracker;
    this.assignmentManager = assignmentManager;
    this.tableLockManager = tableLockManager;
    this.skipTableStateCheck = skipTableStateCheck;
  }

  public EnableTableHandler prepare()
      throws TableNotFoundException, TableNotDisabledException, IOException {
    //acquire the table write lock, blocking
    this.tableLock = this.tableLockManager.writeLock(tableName,
        EventType.C_M_ENABLE_TABLE.toString());
    this.tableLock.acquire();

    boolean success = false;
    try {
      // Check if table exists
      if (!MetaReader.tableExists(catalogTracker, tableName)) {
        // retainAssignment is true only during recovery.  In normal case it is false
        if (!this.skipTableStateCheck) {
          throw new TableNotFoundException(tableName);
        } 
        try {
          this.assignmentManager.getZKTable().removeEnablingTable(tableName, true);
          throw new TableNotFoundException(tableName);
        } catch (KeeperException e) {
          // TODO : Use HBCK to clear such nodes
          LOG.warn("Failed to delete the ENABLING node for the table " + tableName
              + ".  The table will remain unusable. Run HBCK to manually fix the problem.");
        }
      }

      // There could be multiple client requests trying to disable or enable
      // the table at the same time. Ensure only the first request is honored
      // After that, no other requests can be accepted until the table reaches
      // DISABLED or ENABLED.
      if (!skipTableStateCheck) {
        try {
          if (!this.assignmentManager.getZKTable().checkDisabledAndSetEnablingTable
            (this.tableName)) {
            LOG.info("Table " + tableName + " isn't disabled; skipping enable");
            throw new TableNotDisabledException(this.tableName);
          }
        } catch (KeeperException e) {
          throw new IOException("Unable to ensure that the table will be" +
            " enabling because of a ZooKeeper issue", e);
        }
      }
      success = true;
    } finally {
      if (!success) {
        releaseTableLock();
      }
    }
    return this;
  }

  @Override
  public String toString() {
    String name = "UnknownServerName";
    if(server != null && server.getServerName() != null) {
      name = server.getServerName().toString();
    }
    return getClass().getSimpleName() + "-" + name + "-" + getSeqid() + "-" +
        tableName;
  }

  @Override
  public void process() {
    try {
      LOG.info("Attempting to enable the table " + this.tableName);
      MasterCoprocessorHost cpHost = ((HMaster) this.server)
          .getCoprocessorHost();
      if (cpHost != null) {
        cpHost.preEnableTableHandler(this.tableName);
      }
      handleEnableTable();
      if (cpHost != null) {
        cpHost.postEnableTableHandler(this.tableName);
      }
    } catch (IOException e) {
      LOG.error("Error trying to enable the table " + this.tableName, e);
    } catch (KeeperException e) {
      LOG.error("Error trying to enable the table " + this.tableName, e);
    } catch (InterruptedException e) {
      LOG.error("Error trying to enable the table " + this.tableName, e);
    } finally {
      releaseTableLock();
    }
  }

  private void releaseTableLock() {
    if (this.tableLock != null) {
      try {
        this.tableLock.release();
      } catch (IOException ex) {
        LOG.warn("Could not release the table lock", ex);
      }
    }
  }

  private void handleEnableTable() throws IOException, KeeperException, InterruptedException {
    // I could check table is disabling and if so, not enable but require
    // that user first finish disabling but that might be obnoxious.

    // Set table enabling flag up in zk.
    this.assignmentManager.getZKTable().setEnablingTable(this.tableName);
    boolean done = false;
    ServerManager serverManager = ((HMaster)this.server).getServerManager();
    // Get the regions of this table. We're done when all listed
    // tables are onlined.
    List<Pair<HRegionInfo, ServerName>> tableRegionsAndLocations = MetaReader
        .getTableRegionsAndLocations(this.catalogTracker, tableName, true);
    int countOfRegionsInTable = tableRegionsAndLocations.size();
    Map<HRegionInfo, ServerName> regionsToAssign =
        regionsToAssignWithServerName(tableRegionsAndLocations);
    int regionsCount = regionsToAssign.size();
    if (regionsCount == 0) {
      done = true;
    }
    LOG.info("Table '" + this.tableName + "' has " + countOfRegionsInTable
      + " regions, of which " + regionsCount + " are offline.");
    List<ServerName> onlineServers = serverManager.createDestinationServersList();
    Map<ServerName, List<HRegionInfo>> bulkPlan =
        this.assignmentManager.getBalancer().retainAssignment(regionsToAssign, onlineServers);
    LOG.info("Bulk assigning " + regionsCount + " region(s) across " + bulkPlan.size()
      + " server(s), retainAssignment=true");
    
    BulkAssigner ba = new GeneralBulkAssigner(this.server, bulkPlan, this.assignmentManager, true);
    try {
      if (ba.bulkAssign()) {
        done = true;
      }
    } catch (InterruptedException e) {
      LOG.warn("Enable operation was interrupted when enabling table '"
        + this.tableName + "'");
      // Preserve the interrupt.
      Thread.currentThread().interrupt();
    }
    if (done) {
      // Flip the table to enabled.
      this.assignmentManager.getZKTable().setEnabledTable(
        this.tableName);
      LOG.info("Table '" + this.tableName
      + "' was successfully enabled. Status: done=" + done);
    } else {
      LOG.warn("Table '" + this.tableName
      + "' wasn't successfully enabled. Status: done=" + done);
    }
  }

  /**
   * @param regionsInMeta
   * @return List of regions neither in transition nor assigned.
   * @throws IOException
   */
  private Map<HRegionInfo, ServerName> regionsToAssignWithServerName(
      final List<Pair<HRegionInfo, ServerName>> regionsInMeta) throws IOException {
    Map<HRegionInfo, ServerName> regionsToAssign =
        new HashMap<HRegionInfo, ServerName>(regionsInMeta.size());
    RegionStates regionStates = this.assignmentManager.getRegionStates();
    for (Pair<HRegionInfo, ServerName> regionLocation : regionsInMeta) {
      HRegionInfo hri = regionLocation.getFirst();
      ServerName sn = regionLocation.getSecond();
      if (regionStates.isRegionOffline(hri)) {
        regionsToAssign.put(hri, sn);
      } else {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Skipping assign for the region " + hri + " during enable table "
              + hri.getTable() + " because its already in tranition or assigned.");
        }
      }
    }
    return regionsToAssign;
  }
}
