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
import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.concurrent.Callable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tachyon.Constants;
import tachyon.TachyonURI;
import tachyon.Version;
import tachyon.client.TachyonFile;
import tachyon.client.TachyonFS;
import tachyon.client.WriteType;
import tachyon.client.ReadType;
import tachyon.conf.TachyonConf;

public class WordCount implements Callable<Boolean> {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  private final TachyonURI mMasterLocation;
  private final TachyonURI mFileReadPath;
  //private final TachyonURI mFileWrittenPath;
  private final TachyonURI mFileResultPath;
  private final WriteType mWriteType;
  private final ReadType mReadType;
  private final String mFilenamePrefix = "key_value_";
  private final String mFilenameSuffix = ".txt";
  private final int mNumLineIntermediateFile = 100000;
  private HashMap<String, Integer> mWordMap = new HashMap<String, Integer>();
  private int mNumIntermediateFile = 1;
  private String mFlag = "";
  private final String mTargetDir = "test";
  private final String mSentinel = "/" + mTargetDir + "/" + mFilenamePrefix 
      + "SENTINEL" + mFilenameSuffix;

  public WordCount(TachyonURI masterLocation, TachyonURI fileReadPath, 
      TachyonURI fileResultPath,
      WriteType writeType, ReadType readType, String flag) {
    mMasterLocation = masterLocation;
    mFileReadPath = fileReadPath;
    //mFileWrittenPath = fileWrittenPath;
    mFileResultPath = fileResultPath;
    mWriteType = writeType;
    mReadType = readType;
    mFlag = flag;
  }

  @Override
  public Boolean call() throws Exception {
    TachyonFS tachyonClient = TachyonFS.get(mMasterLocation, new TachyonConf());
    TachyonFile fileRead = tachyonClient.getFile(mFileReadPath);
    TachyonFile result = _fileCreate(tachyonClient, mFileResultPath);
        
    //TODO: what the heck is going on?
    if (mFlag.equals("map")) {
      _fileRemove(tachyonClient, "/" + mTargetDir);
      tokenizer(tachyonClient, fileRead);
    } else if (mFlag.equals("reduce")) {
      wordCounter(tachyonClient, result);
    } else {
      LOG.info("An unknown flag " + mFlag + " is given! Will do both map and reduce sequentiailly");
      tokenizer(tachyonClient, fileRead);
      wordCounter(tachyonClient, result);      
    }

    LOG.info("Finish testing...");
    return true;
  }
  
  private void _fileRemove(TachyonFS client, String filename) throws IOException {
    TachyonURI path = new TachyonURI(filename);
    if (client.exist(path)) {
      LOG.info("here");
      try {
        client.delete(path, true);
      } catch (IOException e) {
        LOG.error("delete failed!"); 
      }
    }
  }
  
  private TachyonFile _fileCreate(TachyonFS client, TachyonURI path) throws IOException {
    if (!client.exist(path)) {
      try {
        client.createFile(path);
      } catch (IOException e) {
        LOG.error("createFile failed!"); 
      }
    } else {
      try {
        client.delete(path, true);
        client.createFile(path);
      } catch (IOException e) {
        LOG.error("delete or createFile failed!"); 
      }
    }
    return client.getFile(path);
  }

  private TachyonFile _fileCreate(TachyonFS client, String filename) throws IOException {
    TachyonURI path = new TachyonURI(filename);
    return _fileCreate(client, path);
  }

  private void tokenizer(TachyonFS client, TachyonFile fileRead) throws Exception {
    DataInputStream input = new DataInputStream(fileRead.getInStream(mReadType));
    //DataOutputStream output = new DataOutputStream(fileWritten.getOutStream(mWriteType));
    BufferedReader br = new BufferedReader(new InputStreamReader(input));
    //BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(output));
    String[] tokens = null;
    String lineOfFile = "";
    String filename = "/" + mTargetDir + "/" + mFilenamePrefix 
        + mNumIntermediateFile + mFilenameSuffix;
    
    TachyonFile fileIntermediate = _fileCreate(client, filename);
    DataOutputStream output = new DataOutputStream(fileIntermediate.getOutStream(mWriteType));
    BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(output));
    
    int numTokens = 0;

    while ((lineOfFile = br.readLine()) != null) {
      tokens = lineOfFile.split("\\s+");
      for (String ss: tokens) {
        numTokens++;
        if (numTokens >= mNumLineIntermediateFile) {
          numTokens = 0;
          mNumIntermediateFile++;
          bw.close();
          output.close();
          filename = "/" + mTargetDir + "/" + mFilenamePrefix
              + mNumIntermediateFile + mFilenameSuffix;
          fileIntermediate = _fileCreate(client, filename);
          output = new DataOutputStream(fileIntermediate.getOutStream(mWriteType));
          bw = new BufferedWriter(new OutputStreamWriter(output));
        }
        //LOG.info(ss);
        bw.write(ss + ",1");
        bw.newLine();
      }
    }
    bw.close();
    br.close();
    input.close();
    output.close();
    
    /*
     * Create a sentinel file to notify the end of mapping
     */
    _fileCreate(client, "/" + mTargetDir + "/" + mFilenamePrefix + "SENTINEL" + mFilenameSuffix);
  }

  private void wordCounter(TachyonFS client, TachyonFile fileResult) throws IOException {
    DataOutputStream output = new DataOutputStream(fileResult.getOutStream(mWriteType));
    DataInputStream input = null;
    BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(output));
    BufferedReader br = null;
    String[] tokens = null;
    TachyonFile fileRead = null;
    String lineOfFile = "";

    for (int i = 1; ; i++) {
      String filename = "/" + mTargetDir + "/" + mFilenamePrefix + i + mFilenameSuffix;
      LOG.info(filename);
      try {
        fileRead = client.getFile(new TachyonURI(filename));
        input = new DataInputStream(fileRead.getInStream(mReadType));
        br = new BufferedReader(new InputStreamReader(input));
      } catch (Exception e) {
        if (client.exist(new TachyonURI(mSentinel))) {
          LOG.info("Found Sentinel! Finishing reducing...");
          break;
        } else {
          LOG.info("Didn't find Sentinel, waiting for more inputs...");
          i--;
          continue;
        }
      }
      
      while ((lineOfFile = br.readLine()) != null) {
        tokens = lineOfFile.split(",");
        if (mWordMap.containsKey(tokens[0])) {
          Integer count = (Integer)mWordMap.get(tokens[0]);
          mWordMap.put(tokens[0], new Integer(count.intValue() + 1));
        } else {
          // we haven't seen this word, so add it with count of 1
          mWordMap.put(tokens[0], new Integer(1));
        }
      }
      br.close();
      input.close();
    }

    // now print out every word in the book, along with its count,
    // in alphabetical order
    ArrayList arraylist = new ArrayList(mWordMap.keySet());
    Collections.sort(arraylist);
    
    for (int i = 0; i < arraylist.size(); i++) {
      String key = (String)arraylist.get(i);
      Integer count = (Integer)mWordMap.get(key);
      bw.write(key + "," + count);
      bw.newLine();
    }
    bw.close();
    output.close();
  }

  public static void main(String[] args) throws IllegalArgumentException {
    if (args.length < 5 || args.length > 6) {
      usage();
    }
    
//    String masterAddress = "tachyon://localhost:19998";
//    String fileReadPath = "/big.txt";
//    String fileWrittenPath = "/big_out.txt";
//    String fileResultPath = "/big_result.txt";
//    String readType = "CACHE";
//    String writeType = "TRY_CACHE";

    Utils.runExample(new WordCount(new TachyonURI(args[0]), 
        new TachyonURI(args[1]), 
        new TachyonURI(args[2]), 
        WriteType.valueOf(args[3]), 
        ReadType.valueOf(args[4]),
        args[5]));
  }
  
  private static void usage() {
    System.out.println("USAGE: java -cp target/tachyon-" + Version.VERSION 
        + "-jar-with-dependencies.jar "
        + WordCount.class.getName()
        + " <master address> <read file path> "
        + "<result file path> <write type> <read type> <flag>");
    System.out.println("E.g., java -cp ~/my_projects/tachyon/core/target/"
        + "tachyon-0.7.0-SNAPSHOT-jar-with-dependencies.jar "
        + "tachyon.examples.WordCount tachyon://localhost:19998 /big.txt "
        + "/big_result.txt TRY_CACHE CACHE map");
    System.exit(-1);
  }
}
