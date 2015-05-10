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

/** Description of FileEventTracker 
 * 
 * This class can be used to listen to events on file paths on Tachyon 
 * 
 * @author Thamir Qadah (qadah.thamir@gmail.com)
 */

package tachyon.client;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import tachyon.TachyonURI;
import tachyon.conf.TachyonConf;
import tachyon.thrift.ClientFileInfo;
import tachyon.util.ThreadFactoryUtils;

public class FileEventsTracker {

  private Logger mLog = Logger.getLogger(this.getClass());

  private class EventTrackerEntry {
    private FileEventListener mFEL;
    private ScheduledFuture mSF;
    private int mId;

    public EventTrackerEntry(FileEventListener mFEL, ScheduledFuture mSF, int id) {
      super();
      this.mFEL = mFEL;
      this.mSF = mSF;
      this.mId = id;
    }

    public FileEventListener getFileEventListener() {
      return mFEL;
    }

    public ScheduledFuture getFuture() {
      return mSF;
    }

    /**
     * @return the id
     */
    public int getId() {
      return mId;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Object#hashCode()
     */
    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + getOuterType().hashCode();
      result = prime * result + mId;
      return result;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Object#equals(java.lang.Object)
     */
    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }
      if (obj == null) {
        return false;
      }
      if (getClass() != obj.getClass()) {
        return false;
      }
      EventTrackerEntry other = (EventTrackerEntry) obj;
      if (!getOuterType().equals(other.getOuterType())) {
        return false;
      }
      if (mId != other.mId) {
        return false;
      }
      return true;
    }

    private FileEventsTracker getOuterType() {
      return FileEventsTracker.this;
    }

  }

  private Map<String, Set<EventTrackerEntry>> mEntries = 
      new HashMap<String, Set<EventTrackerEntry>>();

  private ScheduledExecutorService mScheduledExecutorService;

  private TachyonFS mTClient;
  private int mCheckRate;
  private static final int DEFAULT_EXEC_THREAD_COUNT = 4;
  private static final int DEFAULT_RATE_MS = 100;

  public synchronized int registerListener(String path, FileEventListener fl)
      throws IOException {

    Set<EventTrackerEntry> eteSet = mEntries.get(path);
    int res = 0;
    if (eteSet != null) {
      res = eteSet.size();
    } else {
      eteSet = new HashSet<EventTrackerEntry>();
    }

    ScheduledFuture<?> sf = mScheduledExecutorService.scheduleAtFixedRate(
        new FileListenerRunnable(mTClient, new TachyonURI(path), fl), 0,
        mCheckRate, TimeUnit.MILLISECONDS);
    EventTrackerEntry ete = new EventTrackerEntry(fl, sf, res);
    eteSet.add(ete);
    mEntries.put(path, eteSet);
    return res;
  }

  /**
   * Remove listeners for a specific path given the listener id. If id == -1 ,
   * all listeners will be removed.
   * 
   * @param path
   * @param id
   */

  public synchronized void removeListener(String path, int id) {
    Set<EventTrackerEntry> eteSet = mEntries.get(path);
    if (id == -1) {
      // remove all listener
      if (eteSet != null) {
        for (EventTrackerEntry ete : eteSet) {
          ete.getFuture().cancel(false);
        }
        mEntries.remove(path);
      }
    } else {
      if (eteSet != null) {
        for (EventTrackerEntry ete : eteSet) {
          if (id == ete.getId()) {
            ete.getFuture().cancel(false);
            return;
          }
        }
      }
    }
  }

  private void init(TachyonFS mTachyonClient, int checkRate, int threadCount) {
    mScheduledExecutorService = Executors.newScheduledThreadPool(threadCount,
        ThreadFactoryUtils.daemon("client-update-listener-%d"));

    this.mCheckRate = checkRate;
    this.mTClient = mTachyonClient;

  }

  public FileEventsTracker(TachyonFS mTachyonClient) {
    init(mTachyonClient, DEFAULT_RATE_MS, DEFAULT_EXEC_THREAD_COUNT);
  }

  public FileEventsTracker(TachyonFS mTachyonClient, int checkRate) {
    init(mTachyonClient, checkRate, DEFAULT_EXEC_THREAD_COUNT);
  }

  public FileEventsTracker(TachyonFS mTachyonClient, int checkRate,
      int threadCount) {

    init(mTachyonClient, checkRate, threadCount);

  }

  public void shutdown() throws InterruptedException {
    for (Set<EventTrackerEntry> eteSet : mEntries.values()) {
      for (EventTrackerEntry ete : eteSet) {
        ete.getFuture().cancel(false);
      }
    }

    mScheduledExecutorService.awaitTermination(100, TimeUnit.SECONDS);
    mScheduledExecutorService.shutdown();
  }

  private class FileListenerRunnable implements Runnable {

    private TachyonURI mListenUri;
    private TachyonFS mTFS;
    private FileEventListener mFEListener;
    private List<ClientFileInfo> mFiles;
    // private TachyonFile mTfile;
    private ClientFileInfo mFileInfo;
    // private String mTachyonMaster = "tachyon://192.168.56.101:19998";

    private boolean mInitialrun = true;
    
    public FileListenerRunnable(TachyonFS TFS, TachyonURI listenUri,
        FileEventListener el) throws IOException {
      super();
      this.mListenUri = listenUri;
      mTFS = TFS;
      mFileInfo = mTFS.getFileStatus(-1, mListenUri);
      mFEListener = el;
      if (mFileInfo != null) {
        mFiles = mTFS.listStatus(mListenUri);
      }
    }

    @Override
    public void run() {
      // System.out.println("Checking path: " + mListenUri.getPath());
      // Comment
      try {

        ClientFileInfo newFileInfo = mTFS.getFileStatus(-1, mListenUri);
        
        
        
        if (mFileInfo == null) {
          // listening to nonexitent file, can only fire file created events
          // System.out.println("Path does not exist: " + mListenUri.getPath());
          if (mInitialrun) {
            mLog.info("Tracking non-existing path : " + mListenUri.getPath());
            mInitialrun = false;
          }

          if (newFileInfo != null) {
            mFileInfo = newFileInfo;
            mFEListener.onFileCreated();

            
            if (newFileInfo.isFolder) {
              mFiles = mTFS.listStatus(mListenUri);
            }
            
            return;
          }

        } else {

          if (mInitialrun) {
            if (mFileInfo.isFolder) {
              mLog.info("Tracking a directory : " + mListenUri.getPath());
            } else {
              mLog.info("Tracking a file : " + mListenUri.getPath());
            }

            mInitialrun = false;
          }

          if (newFileInfo == null) {
            mFileInfo = newFileInfo;
            mFEListener.onFileDeleted();
            return;
          }
          // System.out.println("new file info not null");

          if (mFileInfo.getLastModificationTimeMs() < newFileInfo
              .getLastModificationTimeMs()) {
            mFileInfo = newFileInfo;
            mFEListener.onFileUpdate(newFileInfo);

          }
          
          if (mFileInfo.isFolder) {
            
            List<ClientFileInfo> newFiles = mTFS.listStatus(new TachyonURI(
                mFileInfo.path));
            List<ClientFileInfo> mDiff = new ArrayList<ClientFileInfo>();

            // mLog.info(Thread.currentThread().getId() +
            // " : is checking list status");

            // TODO: optimize
            
//            mLog.info(Thread.currentThread().getId()
//                + " : new files size :" + newFiles.size());
            
            
            for (ClientFileInfo nci : newFiles) {
              if (nci.isComplete) {
                mDiff.add(nci);
              }                  
            }
            
            mDiff.removeAll(mFiles);
            

            if (mDiff.size() > 0) {
              mFEListener.onFileListUpdate(newFileInfo, mDiff);
            }
            
//            mLog.info(Thread.currentThread().getId()
//                + " : old files size :" + mFiles.size());
            
            mFiles.addAll(mDiff);
            
            
//            mLog.info(Thread.currentThread().getId()
//                + " : update list is handled : " + mFiles.size());
          }

        }

      } catch (IOException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }

    }

  }

}
