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
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.concurrent.Callable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tachyon.Constants;
import tachyon.TachyonURI;
import tachyon.Version;
import tachyon.client.OutStream;
import tachyon.client.TachyonByteBuffer;
import tachyon.client.TachyonFile;
import tachyon.client.TachyonFS;
import tachyon.client.WriteType;
import tachyon.client.ReadType;
import tachyon.client.FileInStream;
import tachyon.client.FileOutStream;
import tachyon.conf.TachyonConf;
import tachyon.util.CommonUtils;

public class WordCount implements Callable<Boolean> {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  private final TachyonURI mMasterLocation;
  private final TachyonURI mFileReadPath;
  private final TachyonURI mFileWrittenPath;
  private final TachyonURI mFileResultPath;
  private final WriteType mWriteType;
  private final ReadType mReadType;
  private final int mNumbers = 20;
  HashMap<String, Integer> mWordMap = new HashMap<String, Integer>();

  public WordCount(TachyonURI masterLocation, TachyonURI fileReadPath, 
      TachyonURI fileWrittenPath, 
      TachyonURI fileResultPath,
      WriteType writeType, ReadType readType) {
    mMasterLocation = masterLocation;
    mFileReadPath = fileReadPath;
    mFileWrittenPath = fileWrittenPath;
    mFileResultPath = fileResultPath;
    mWriteType = writeType;
    mReadType = readType;
  }

  @Override
  public Boolean call() throws Exception {
    TachyonFS tachyonClient = TachyonFS.get(mMasterLocation, new TachyonConf());
    TachyonFile fileRead = tachyonClient.getFile(mFileReadPath);
    TachyonByteBuffer inbuf = null;
    ByteBuffer outbuf = ByteBuffer.allocate((int)fileRead.getBlockSizeByte());
    String s = "";
    char c;
    
    outbuf.order(ByteOrder.nativeOrder());
    
    
    if (!tachyonClient.exist(mFileWrittenPath)) {
      try {
        tachyonClient.createFile(mFileWrittenPath);
      } catch (IOException e) {
        LOG.error("createFile failed!"); 
      }
    } else {
      try {
        tachyonClient.delete(mFileWrittenPath, true);
        tachyonClient.createFile(mFileWrittenPath);
      } catch (IOException e) {
        LOG.error("delete or createFile failed!"); 
      }
    }

    TachyonFile fileWritten = tachyonClient.getFile(mFileWrittenPath);
    tokenizer(fileRead, fileWritten);

    if (!tachyonClient.exist(mFileResultPath)) {
      try {
        tachyonClient.createFile(mFileResultPath);
      } catch (IOException e) {
        LOG.error("createFile failed!"); 
      }
    } else {
      try {
        tachyonClient.delete(mFileResultPath, true);
        tachyonClient.createFile(mFileResultPath);
      } catch (IOException e) {
        LOG.error("delete or createFile failed!"); 
      }
    }
    TachyonFile result = tachyonClient.getFile(mFileResultPath);
    wordCounter(fileWritten, result);

    //int numberOfBlocks = fileRead.getNumberOfBlocks();
    //OutStream fOutStream = fileWritten.getOutStream(mWriteType);
    //String debugS = "";
    
    //DataInputStream input = new DataInputStream(fileRead.getInStream(mReadType));
    //DataOutputStream output = new DataOutputStream(fileWritten.getOutStream(mWriteType));
    //BufferedReader bd = new BufferedReader(new InputStreamReader(input));
    //BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(output));
    //String[] tokens = null;
    //while ((debugS = bd.readLine()) != null) {
    //  tokens = debugS.split("\\s+");
    //  for (String ss: tokens) {
    //    //LOG.info(ss);
    //    bw.write(ss + ",1");
    //    bw.newLine();
    //  }
    //}
    //bw.close();
    //bd.close();
    //input.close();
    //output.close();

    

    //for (int i = 0; i < numberOfBlocks; i ++) {
    //  inbuf = fileRead.readByteBuffer(i);
    //  if (inbuf == null) {
    //    fileRead.recache();
    //    inbuf = fileRead.readByteBuffer(i);
    //  }
    //  try {
    //    outbuf.put(inbuf.mData);
    //  } catch (BufferOverflowException e) {
    //    LOG.error("outbuf overflowed!");
    //  }
    //  
    //  while (outbuf.hasRemaining()) {
    //    c = outbuf.getChar();
    //    System.out.println((int)c);
    //    debugS += c;
    //    //LOG.info(debugS);
    //    debugS = "";

    //    if (!Character.isWhitespace(c)) {
    //      s += c;
    //    } else {
    //      if (map.containsKey(s)) {
    //        Integer count = (Integer)map.get(s);
    //        map.put(s, new Integer(count.intValue() + 1));
    //      } else {
    //        // we haven't seen this word, so add it with count of 1
    //        map.put(s, new Integer(1));
    //      }
    //      s = "";
    //    }
    //  }
    //  //s = new String(outbuf.array());
    //  //System.out.println("============>" + s);
//  //    outbuf.flip();
//  //    outbuf.flip();
//  //    
//  //    if (outbuf.hasArray()) {
//  //      fOutStream.write(outbuf.array());
//  //    } else {
//  //      LOG.error("outbuf does not have backing array!");
//  //    }
    //}

    //// now print out every word in the book, along with its count,
    //// in alphabetical order
    //ArrayList arraylist = new ArrayList(map.keySet());
    //Collections.sort(arraylist);
    //
    //for (int i = 0; i < arraylist.size(); i++) {
    //  String key = (String)arraylist.get(i);
    //  Integer count = (Integer)map.get(key);
    //  System.out.println(key + " --> " + count);
    //}
    
    //fOutStream.flush();
    //fOutStream.close();

    LOG.info("Just testing...");
    return true;
    //createFile(tachyonClient);
    //writeFile(tachyonClient);
    //return readFile(tachyonClient);
  }

  private TachyonFile tokenizer(TachyonFile fileRead, TachyonFile fileWritten) throws Exception {
    DataInputStream input = new DataInputStream(fileRead.getInStream(mReadType));
    DataOutputStream output = new DataOutputStream(fileWritten.getOutStream(mWriteType));
    BufferedReader bd = new BufferedReader(new InputStreamReader(input));
    BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(output));
    String[] tokens = null;
    String debugS = "";

    while ((debugS = bd.readLine()) != null) {
      tokens = debugS.split("\\s+");
      for (String ss: tokens) {
        //LOG.info(ss);
        bw.write(ss + ",1");
        bw.newLine();
      }
    }
    bw.close();
    bd.close();
    input.close();
    output.close();
    return fileWritten;
  }

  private void wordCounter(TachyonFile fileRead, TachyonFile fileWritten) throws Exception {
    DataInputStream input = new DataInputStream(fileRead.getInStream(mReadType));
    DataOutputStream output = new DataOutputStream(fileWritten.getOutStream(mWriteType));
    BufferedReader bd = new BufferedReader(new InputStreamReader(input));
    BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(output));
    String[] tokens = null;
    String debugS = "";

    while ((debugS = bd.readLine()) != null) {
      tokens = debugS.split(",");
      if (mWordMap.containsKey(tokens[0])) {
        Integer count = (Integer)mWordMap.get(tokens[0]);
        mWordMap.put(tokens[0], new Integer(count.intValue() + 1));
      } else {
        // we haven't seen this word, so add it with count of 1
        mWordMap.put(tokens[0], new Integer(1));
      }
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
    bd.close();
    input.close();
    output.close();
  }

  public static void main(String[] args) throws IllegalArgumentException {
    if (args.length != 6) {
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
        new TachyonURI(args[3]),
        WriteType.valueOf(args[4]), 
        ReadType.valueOf(args[5])));
  }
  
  private static void usage() {
    System.out.println("USAGE: java -cp target/tachyon-" + Version.VERSION 
        + "-jar-with-dependencies.jar "
        + WordCount.class.getName()
        + " <master address> <read file path> <intermediate write file path> "
        + "<result file path> <write type> <read type>");
    System.out.println("E.g., java -cp ~/my_projects/tachyon/core/target/"
        + "tachyon-0.7.0-SNAPSHOT-jar-with-dependencies.jar "
        + "tachyon.examples.WordCount tachyon://localhost:19998 /big.txt "
        + "/big_out.txt /big_result.txt TRY_CACHE CACHE");
    System.exit(-1);
  }
}
