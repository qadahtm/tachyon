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
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import tachyon.TachyonURI;
import tachyon.client.FileEventListener;
import tachyon.client.TachyonFS;
import tachyon.client.TachyonFile;
import tachyon.conf.TachyonConf;
import tachyon.thrift.ClientFileInfo;
import tachyon.util.ThreadFactoryUtils;

public class BasicFileEventListener {

  private static ScheduledExecutorService sScheduledExecutorService = Executors
      .newScheduledThreadPool(10, ThreadFactoryUtils.daemon("fe-sched-%d"));

  public static void main(String[] args) throws IOException, InterruptedException {

    FileListener fl = new FileListener(new TachyonURI(
        "tachyon://192.168.56.101:19998/random-test-data/"),
        new FileEventListener() {

          @Override
          public void onFileUpdate(ClientFileInfo newFileInfo) {
            // TODO Auto-generated method stub
            System.out.println("file updated");
            
          }

          @Override
          public void onFileDeleted() {
            // TODO Auto-generated method stub
            System.out.println("file deleted");
          }

          @Override
          public void onFileCreated() {
            // TODO Auto-generated method stub
            System.out.println("file Created");
          }

          @Override
          public void onBlockReady(long blockId) {
            // TODO Auto-generated method stub
            System.out.println("block ready");
          }
        });

    ScheduledFuture<?> sf = sScheduledExecutorService.scheduleAtFixedRate(fl,
        0, 300, TimeUnit.MILLISECONDS);
    
    
    System.out.println("Waiting for task termination");
    Thread.sleep(100000);
    sf.cancel(false);
    sScheduledExecutorService.awaitTermination(100, TimeUnit.SECONDS);
    sScheduledExecutorService.shutdown();
    

  }

  private static class FileListener implements Runnable {

    private TachyonURI mListenUri;
    private TachyonFS mTFS;
    private FileEventListener mFEListener;
    private TachyonFile mTf;
    private ClientFileInfo mFileInfo;
    private String mTachyonMaster = "tachyon://192.168.56.101:19998";

    public FileListener(TachyonURI uri, FileEventListener el)
        throws IOException {
      super();
      this.mListenUri = uri;
      mTFS = TachyonFS.get(new TachyonURI(mTachyonMaster), new TachyonConf());
      mFEListener = el;
      mFileInfo = mTFS.getFileStatus(-1, mListenUri);
    }

    @Override
    public void run() {
//      System.out.println("Checking path: " + mListenUri.getPath());
      // Comment
      try {
        
        ClientFileInfo newFileInfo = mTFS.getFileStatus(-1, mListenUri);

        if (mFileInfo == null) {
          // listening to nonexitent file, can only fire file created events
          
          if (newFileInfo != null) {
            mFileInfo = newFileInfo;
            mFEListener.onFileCreated();
            return;
          }

        } else {

          if (newFileInfo == null) {
            mFileInfo = newFileInfo;
            mFEListener.onFileDeleted();
            return;
          }
//          System.out.println("new file info not null");
            
          if (mFileInfo.getLastModificationTimeMs() < newFileInfo
              .getLastModificationTimeMs()) {
            mFileInfo = newFileInfo;
            mFEListener.onFileUpdate(newFileInfo);            
          }

        }

      } catch (IOException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }

    }

  }

}
