package com.liquidm.camus;

import com.linkedin.camus.coders.CamusWrapper;
import com.linkedin.camus.etl.IEtlKey;
import com.linkedin.camus.etl.RecordWriterProvider;
import com.linkedin.camus.etl.kafka.mapred.EtlMultiOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.compress.*;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.util.thread.QueuedThreadPool;
import org.eclipse.jetty.server.Server;


import java.io.IOException;

public class DruidWriter implements RecordWriterProvider {
  final private Writable _emptyKey = NullWritable.get();

  @Override
  public String getFilenameExtension() {
    return "";
  }

  @SuppressWarnings("rawtypes")
  @Override
  public RecordWriter<IEtlKey, CamusWrapper> getDataRecordWriter(TaskAttemptContext context, String fileName, CamusWrapper data, FileOutputCommitter committer) throws IOException, InterruptedException {
      final QueuedThreadPool threadPool = new QueuedThreadPool();
      threadPool.setMinThreads(5);
      threadPool.setMaxThreads(100);

      final Server server = new Server(threadPool);
      final ServerConnector connector = new ServerConnector(server);
      server.setConnectors(new Connector[]{connector});

      try {
          server.start();
          System.out.println("Jetty on " + connector.getLocalPort());
      } catch (Exception e) {
          throw new IOException(e);
      }

      return new RecordWriter<IEtlKey, CamusWrapper>() {
          @Override
          public void write(IEtlKey iEtlKey, CamusWrapper camusWrapper) throws IOException, InterruptedException {
          }

          @Override
          public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
              try {
                  server.stop();
              } catch (Exception e) {
                  e.printStackTrace();
              }
          }
      };
  }
}
