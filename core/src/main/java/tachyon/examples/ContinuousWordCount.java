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

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;

import tachyon.TachyonURI;
import tachyon.client.FileEventListener;
import tachyon.client.FileEventsTracker;
import tachyon.client.InStream;
import tachyon.client.OutStream;
import tachyon.client.ReadType;
import tachyon.client.TachyonFS;
import tachyon.client.TachyonFile;
import tachyon.client.WriteType;
import tachyon.conf.TachyonConf;
import tachyon.thrift.ClientFileInfo;

public class ContinuousWordCount {

  Hashtable<String, Long> mResults = new Hashtable<String, Long>();

  private static class WCMap implements Runnable {

    private ClientFileInfo mFileInfo;
    private TachyonFS mTFS;
    private String mOutPath;

    public WCMap(ClientFileInfo fileinfo, TachyonFS tfs, String outpath) {
      this.mFileInfo = fileinfo;
      this.mTFS = tfs;
      this.mOutPath = outpath;
    }

    @Override
    public void run() {
      try {

        System.out.println("Mapper - Processing a new File : " + mFileInfo.getPath());
        TachyonFile in = mTFS.getFile(mFileInfo.id);
        
        String outfile = mFileInfo.getPath().substring(
            mFileInfo.getPath().lastIndexOf("/") + 1);
        String foutpath = 
            mTFS.getUri().getPath() + mOutPath + "/" + outfile;
//        System.out.println("mapper output path : " + foutpath);
        TachyonURI fouturi = new TachyonURI(foutpath);
        
        //check if exists
        TachyonFile out = mTFS.getFile(fouturi);
        
        if (out != null) {          
          mTFS.delete(fouturi, true);          
        }
        
        out = mTFS.getFile(mTFS.createFile(fouturi));
        
        OutStream os = out.getOutStream(WriteType.ASYNC_THROUGH);

        BufferedReader bf = new BufferedReader(new InputStreamReader(
            in.getInStream(ReadType.CACHE)));

        String line = bf.readLine();
        while (line != null) {
          String[] arr = line.split("\\s");
          for (String w : arr) {
            String tup = w + "," + "1\n";
            os.write(tup.getBytes());
          }
          line = bf.readLine();
        }

        os.close();
        System.out.println("Mapper - Finished Processing : " + mFileInfo.getPath());

      } catch (IOException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }

    }

  }

  private static class WCReduce implements Runnable {

    private ClientFileInfo mFileInfo;
    private TachyonFS mTFS;
    private String mOutPath;
    private Hashtable<String, Long> mResults = new Hashtable<String, Long>();
    
    private boolean mTest = true;

    public WCReduce(ClientFileInfo fileinfo, TachyonFS tfs, String outpath) {
      this.mFileInfo = fileinfo;
      this.mTFS = tfs;
      this.mOutPath = outpath;
    }

    @Override
    public void run() {
      try {

        System.out.println("Reducer - Processing a new File : " + mFileInfo.getPath());
        TachyonFile in = mTFS.getFile(mFileInfo.id);
        String outfile = mFileInfo.getPath().substring(
            mFileInfo.getPath().lastIndexOf("/") + 1);
        String foutpath = 
            mTFS.getUri().getPath() + mOutPath + "/" + outfile;
        
        TachyonURI outuri = new TachyonURI(foutpath);
        TachyonFile out =  mTFS.getFile(outuri);
        
        if (out == null) {
          out = mTFS.getFile(mTFS.createFile(outuri));       
        } else {
          // load from previous iteration
          
          System.out.println("Reducer - Loading previous results : " + out.getPath());
          
          BufferedReader bfl = new BufferedReader(new InputStreamReader(
              out.getInStream(ReadType.CACHE)));
          String line = bfl.readLine();
          while (line != null) {
            String[] arr = line.split(",");
            Long ov = mResults.get(arr[0]);
            if (ov == null) {
              mResults.put(arr[0], Long.parseLong(arr[1]));
            } else {
              mResults.put(arr[0], Long.parseLong(arr[1]) + ov);
            }

            line = bfl.readLine();
          }
          
          mTFS.delete(outuri, true);
          out = mTFS.getFile(mTFS.createFile(outuri));
          
          System.out.println("Reducer - Finished loading previous results : " + out.getPath());
        }
        
        
        BufferedReader bf = new BufferedReader(new InputStreamReader(
            in.getInStream(ReadType.CACHE)));

        String line = bf.readLine();
        while (line != null) {
          String[] arr = line.split(",");
          Long ov = mResults.get(arr[0]);
          if (ov == null) {
            mResults.put(arr[0], Long.parseLong(arr[1]));
          } else {
            mResults.put(arr[0], Long.parseLong(arr[1]) + ov);
          }

          line = bf.readLine();
        }
        
        // Write results
        OutStream os = out.getOutStream(WriteType.ASYNC_THROUGH);
        for (String key : mResults.keySet()) {
          String tup = key + "," + mResults.get(key) + "\n";
          os.write(tup.getBytes());
        }
        os.close();
        
        System.out.println("Reducer - Finished Processing : " + mFileInfo.getPath());

      } catch (IOException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
      
    }

  }

  public static void main(String[] args) throws IOException,
      InterruptedException {

    String mTachyonMaster = args[0];
    String path = args[1];
    String mpath = path;
    if (path.lastIndexOf("/") == path.length() - 1) {
      mpath = path.substring(0, path.length() - 1);
    }
    final String mappath = mpath.substring(mpath.lastIndexOf("/") + 1) + "_map";
    final String reducepath = mpath.substring(mpath.lastIndexOf("/") + 1) + "_reduce";
    

    final TachyonFS tfs = TachyonFS.get(new TachyonURI(mTachyonMaster),
        new TachyonConf());

    FileEventsTracker ftracker = new FileEventsTracker(tfs);

    FileEventListener maplistener = new FileEventListener() {

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
          (new Thread(new WCMap(f, tfs, mappath))).start();

        }

      }

    };
    
    FileEventListener reducelistener = new FileEventListener() {

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
          (new Thread(new WCReduce(f, tfs, reducepath))).start();

        }

      }

    };
    
    
    
    System.out.println("stage 1 path : " + path);
    System.out.println("stage 2 path : " + mTachyonMaster + "/" + mappath);
    
    int listen1id = ftracker.registerListener(path, maplistener);
    int listen2id = ftracker.registerListener(mTachyonMaster + "/" + mappath,
        reducelistener);

    Thread.sleep(100000);
    ftracker.shutdown();

  }

}
