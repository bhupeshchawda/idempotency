package com.example.idempotencyTester;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.Operator.CheckpointNotificationListener;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.common.util.BaseOperator;

public class IdempotencyTesterOperator extends BaseOperator implements CheckpointNotificationListener
{
  private transient BufferedWriter bw;
  private String logFilePath = "/tmp/log";
  private transient FileSystem fileSystem;
  private transient long currentWindowId;

  @Override
  public void setup(OperatorContext context)
  {
    try {
      fileSystem = FileSystem.newInstance(new Configuration());
      if (! fileSystem.exists(new Path(logFilePath))) {
        bw = new BufferedWriter(new FileWriter(new File(logFilePath), true));
        bw.write("START_OPERATOR\n");
        bw.flush();
      } else {
        bw = new BufferedWriter(new FileWriter(new File(logFilePath), true));
        bw.write("START_RECOVERY\n");
        bw.flush();
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void beginWindow(long windowId)
  {
    currentWindowId = windowId;
    try {
      bw.write("START_WINDOW" + "\t" + currentWindowId + "\n");
      bw.flush();
    } catch(Exception e) {
      throw new RuntimeException(e);
    }
  }

  public final transient DefaultInputPort<byte[]> input = new DefaultInputPort<byte[]>()
  {
    @Override
    public void process(byte[] tuple)
    {
      try {
        bw.write(currentWindowId + "\t" + new String(tuple) + "\n");
        bw.flush();
      } catch(Exception e) {
        throw new RuntimeException(e);
      }
    }
  };

  @Override
  public void endWindow()
  {
    try {
      bw.write("END_WINDOW" + "\t" + currentWindowId + "\n");
      bw.flush();
    } catch(Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void beforeCheckpoint(long windowId)
  {
    try {
      bw.write("CHECKPOINT_WINDOW" + "\t" + currentWindowId + "\n");
      bw.flush();
    } catch(Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void checkpointed(long windowId)
  {
    
  }

  @Override
  public void committed(long windowId)
  {
    
  }

  @Override
  public void teardown()
  {
    try {
      bw.flush();
      bw.close();
    } catch(Exception e) {
      throw new RuntimeException(e);
    }
  }
}
