/*
 * Licensed to the University of California, Berkeley under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package tachyon.examples;

import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import tachyon.TachyonURI;
import tachyon.client.TachyonFS;
import tachyon.client.TachyonFile;
import tachyon.client.FileEventListener;
import tachyon.conf.TachyonConf;
import tachyon.thrift.ClientFileInfo;
import tachyon.util.ThreadFactoryUtils;

public class FileUpdateOperations {

  /*
   * by tqadah: implemneting update listeners
   */
  private Map<String, FileEventListener> mUpdateListeners = 
        new HashMap<String, FileEventListener>();
  private Map<String, ScheduledFuture> mUpdatePoolers = new HashMap<String, ScheduledFuture>();

  private final ScheduledExecutorService mScheduledExecutorService =
      Executors.newScheduledThreadPool(10,
      ThreadFactoryUtils.daemon("client-update-listener-%d"));
  
  private TachyonFS mTClient;
  private int mCheckRate;
  
  
  public synchronized void _registerListener(String path, FileEventListener ul) {
    mUpdateListeners.put(path, ul);
  }
  
  public synchronized void removeListener(String path) {
    mUpdateListeners.remove(path);
  }
  
  
  public FileUpdateOperations(TachyonFS mTachyonClient, int checkRate) {
    this.mCheckRate =  checkRate;
    this.mTClient = mTachyonClient;    
  }
  
  public static void main(String[] args) throws InterruptedException, IOException {
    
    TachyonURI turi = new TachyonURI("tachyon://localhost:19998");
    final TachyonFS tclient = TachyonFS.get(turi, new TachyonConf());
    
    FileUpdateOperations fo = new FileUpdateOperations(tclient, 2);
    
    ScheduledFuture sf = fo.mScheduledExecutorService.scheduleAtFixedRate(new Runnable(){

      @Override
      public void run() {
        System.out.println("Now is : " + (new Date()));
        System.out.println("Hello from java");
        
        try {
          TachyonFile tf = tclient.getFile(new TachyonURI("/"));
          ClientFileInfo cfi = tclient.getFileStatus(-1, new TachyonURI(
              "/tweet_us_2013_1_3.chicago.csv"));
          cfi.getLastModificationTimeMs();
          

          
        } catch (IOException e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
        }
        
      }}, 0,2, TimeUnit.SECONDS);
    
    System.out.println("Sleeping for 10 seconds");   
    Thread.sleep(10000);
    System.out.println("Woke up .. canceling scheduled runnable.");
    sf.cancel(false);
    fo.mScheduledExecutorService.shutdown();
  }
 

}
