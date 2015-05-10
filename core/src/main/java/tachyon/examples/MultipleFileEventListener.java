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
import tachyon.client.FileEventsTracker;
import tachyon.client.TachyonFS;
import tachyon.client.TachyonFile;
import tachyon.conf.TachyonConf;
import tachyon.thrift.ClientFileInfo;
import tachyon.util.ThreadFactoryUtils;

public class MultipleFileEventListener {

  public static void main(String[] args) throws IOException,
      InterruptedException {
    String mTachyonMaster = "tachyon://192.168.56.101:19998";
    TachyonFS tfs = TachyonFS.get(new TachyonURI(mTachyonMaster),
        new TachyonConf());

    FileEventsTracker ftracker = new FileEventsTracker(tfs);
    String path = "tachyon://192.168.56.101:19998/random-test-data/";

    FileEventListener listener1 = new FileEventListener() {

      @Override
      public void onFileUpdate(ClientFileInfo newFileInfo) {
        System.out.println("file updated");

      }

      @Override
      public void onFileDeleted() {
        System.out.println("file deleted");
      }

      @Override
      public void onFileCreated() {
        System.out.println("file Created");
      }

      @Override
      public void onFileListUpdate(ClientFileInfo newFileInfo,
          List<ClientFileInfo> files) {
        System.out.println("file list updated, file count = " + files.size());

      }

    };

    FileEventListener listener2 = new FileEventListener() {

      @Override
      public void onFileUpdate(ClientFileInfo newFileInfo) {
        System.out.println("file updated2");

      }

      @Override
      public void onFileDeleted() {
        System.out.println("file deleted2");
      }

      @Override
      public void onFileCreated() {
        System.out.println("file Created2");
      }

      @Override
      public void onFileListUpdate(ClientFileInfo newFileInfo,
          List<ClientFileInfo> files) {
        System.out.println("file list updated 2, file count = " + files.size());

      }

    };

    int listen1id = ftracker.registerListener(path, listener1);
    int listen2id = ftracker.registerListener(path, listener2);

    Thread.sleep(100000);
    ftracker.shutdown();

  }

}
