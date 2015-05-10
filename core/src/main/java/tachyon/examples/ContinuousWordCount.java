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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;

import tachyon.TachyonURI;
import tachyon.client.FileEventListener;
import tachyon.client.FileEventsTracker;
import tachyon.client.InStream;
import tachyon.client.ReadType;
import tachyon.client.TachyonFS;
import tachyon.client.TachyonFile;
import tachyon.conf.TachyonConf;
import tachyon.thrift.ClientFileInfo;

public class ContinuousWordCount {

  public static void main(String[] args) throws IOException, InterruptedException {
    
    String mTachyonMaster = args[0];
    String path = args[1];
    
    final TachyonFS tfs = TachyonFS.get(new TachyonURI(mTachyonMaster),
        new TachyonConf());
    
    FileEventsTracker ftracker = new FileEventsTracker(tfs);
    
    FileEventListener listener1 = new FileEventListener() {

      @Override
      public void onFileUpdate(ClientFileInfo newFileInfo) {
        // TODO Auto-generated method stub
        
      }

      @Override
      public void onFileDeleted() {
        // TODO Auto-generated method stub
        
      }

      @Override
      public void onFileCreated() {
        // TODO Auto-generated method stub
        
      }

      @Override
      public void onFileListUpdate(ClientFileInfo newFileInfo,
          List<ClientFileInfo> files) {
        
        for (ClientFileInfo f : files) {
          System.out.println("processing new file: " + f.getPath());
          
          try {
            InStream in = tfs.getFile(f.id).getInStream(ReadType.CACHE);
            BufferedReader bf = new BufferedReader(new InputStreamReader(in));
            String line = bf.readLine();
            for (int i = 0; i < 10; i++) {
              System.out.println(line);
              line = bf.readLine();
            }
            
          } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
          }
          
          
        }
        
      }

    };
    
    int listen1id = ftracker.registerListener(path, listener1);
    
    Thread.sleep(100000);
    ftracker.shutdown();
    
  }

}
