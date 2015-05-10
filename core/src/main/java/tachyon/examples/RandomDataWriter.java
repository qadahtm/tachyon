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
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.fluttercode.datafactory.impl.DataFactory;

import tachyon.TachyonURI;
import tachyon.client.OutStream;
import tachyon.client.TachyonFS;
import tachyon.client.TachyonFile;
import tachyon.client.WriteType;
import tachyon.conf.TachyonConf;
import tachyon.util.ThreadFactoryUtils;

public class RandomDataWriter {

  private static ScheduledExecutorService sScheduledExecutorService = Executors
      .newScheduledThreadPool(10, ThreadFactoryUtils.daemon("random-sched-%d"));

  public static void main(String[] args) throws InterruptedException,
      IOException {
    
    String outputdirpath = "/random-test-data/";
    String outputfile = "data.txt";
    String tachyonMasterUri = "tachyon://192.168.56.101:19998";
    
    boolean closefile = true;
    boolean addnew = true;
    
//    String outputPath = "tachyon://192.168.56.101:19998/random-test-data/data.txt";
    String outputPath = tachyonMasterUri + outputdirpath + outputfile;
    
    
    TachyonConf tconf = new TachyonConf();
    tconf.set("tachyon.user.default.block.size.byte", "4194304"); // 4MB blocks
    
//    tconf.set("tachyon.user.default.block.size.byte", "1048576"); // 1MB blocks
    
    
    final TachyonFS mTFS = TachyonFS.get(new TachyonURI(
        tachyonMasterUri), tconf);
    
    
    DataFactory mDf = new DataFactory();
    
    
    DataAppender appendTask = new DataAppender(mDf, mTFS, outputPath, closefile, addnew);
//    DataAppender appendTask = new DataAppender(mDf, mTFS, outputPath, false, false);
//    DataAppender appendTask = new DataAppender(mDf, mTFS, outputPath, false, true);

    ScheduledFuture<?> sf = sScheduledExecutorService.schedule(appendTask, 1,
        TimeUnit.SECONDS);

    System.out.println("Waiting for task termination");

    sScheduledExecutorService.awaitTermination(10, TimeUnit.SECONDS);
    sScheduledExecutorService.shutdown();

  }
  
  private static class DataAppender implements Runnable {

    private String mOutputPath;
    private DataFactory mDf;
    private TachyonFS mTFS;
    private TachyonFile mTargetFile;
    private int mFi = 0;
    private boolean mCloseFile = true;
    private boolean mAddNewFile = true;
    
    private static final WriteType DEFAULT_WRITE_TYPE = WriteType.ASYNC_THROUGH;

    public DataAppender(DataFactory mDf, TachyonFS mTFS, String outputPath,
        boolean closeFile, boolean addNew) {
      super();
      this.mDf = mDf;
      this.mTFS = mTFS;
      this.mOutputPath = outputPath;
      this.mCloseFile = closeFile;
      this.mAddNewFile = addNew;
      
    }

    public String createEntry() {
      int minX = 0;
      int maxX = 10000;
      int minY = 0;
      int maxY = 10000;

      StringBuilder sb = new StringBuilder();
      int linelen = (int) Math.random() * 8 + 3;
      sb.append(mDf.getRandomWord());
      for (int i = 1; i < linelen; i++) {
        sb.append(" ");
        sb.append(mDf.getRandomWord());
      }
      sb.append("\n");
      sb.append(mDf.getBusinessName() + " located at " + mDf.getAddress());
      sb.append("\n");
      sb.append(getRandomPoint(minX, maxX, minY, maxY));

      // System.out.println(sb.toString());
      return sb.toString();
    }

    @Override
    public void run() {
      // TODO Auto-generated method stub

      String outputPathi = mOutputPath + "." + mFi;
      try {
        TachyonFile tf = mTFS.getFile(new TachyonURI(outputPathi));

        if (mAddNewFile) {
          while (tf != null) {
            
            System.out.println(outputPathi + " already exist, creating a new one");
            mFi++;
            outputPathi = mOutputPath + "." + mFi;
            tf = mTFS.getFile(new TachyonURI(outputPathi));
          }
          
        } else {
          if (tf != null) {
            System.out.println(outputPathi + " already exist, deleting it");
            mTFS.delete(new TachyonURI(outputPathi), true);
          }
          tf = null;
        }
        
        if (tf == null) {
          tf = mTFS.getFile(mTFS.createFile(new TachyonURI(outputPathi)));
        }
               

        OutStream os = tf.getOutStream(DEFAULT_WRITE_TYPE);

        for (int j = 0; j < 2; j++) {
          for (int i = 0; i < 100000; i++) {
            os.write(createEntry().getBytes());
          }
//          Thread.sleep(300);
        }

        if (mCloseFile) {
          os.close();
          System.out.println("Wrote " + outputPathi);
        } else {
          System.out.println("Wrote " + outputPathi + " , not closed");
        }
        
        mFi++;

      } catch (IOException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
//      } catch (InterruptedException e) {
        // TODO Auto-generated catch block
//        e.printStackTrace();
      }

    }

  }

  private static String getRandomPoint(int minX, int maxX, int minY, int maxY) {
    int rX = (int) (Math.random() * maxX) + minX;
    int rY = (int) (Math.random() * maxY) + minY;
    return rY + "," + rX;
  }

}
