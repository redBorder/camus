package com.liquidm.camus;

import com.linkedin.camus.coders.CamusWrapper;
import com.linkedin.camus.etl.IEtlKey;
import com.linkedin.camus.etl.RecordWriterProvider;
import com.linkedin.camus.etl.kafka.mapred.EtlMultiOutputFormat;
import com.liquidm.Events;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import parquet.hadoop.ParquetWriter;
import parquet.hadoop.metadata.CompressionCodecName;
import parquet.proto.ProtoWriteSupport;

import java.io.IOException;

/**
 * Created by sixtus on 14.03.14.
 */
class ParquetCamusWriter implements RecordWriterProvider {

    @Override
    public String getFilenameExtension() {
        return ".parquet";
    }


    @Override
    public RecordWriter<IEtlKey, CamusWrapper> getDataRecordWriter(TaskAttemptContext context, String fileName, CamusWrapper data, FileOutputCommitter committer) throws IOException, InterruptedException {
        ProtoWriteSupport support = new ProtoWriteSupport(Events.EventLogged.class);

        Path cwd = committer.getWorkPath();
        Path file = new Path(cwd, EtlMultiOutputFormat.getUniqueFile(context, fileName, getFilenameExtension()));

        final ParquetWriter<Events.EventLogged> writer = new ParquetWriter<Events.EventLogged>(file, support, CompressionCodecName.GZIP,  ParquetWriter.DEFAULT_BLOCK_SIZE, ParquetWriter.DEFAULT_PAGE_SIZE, false, false);

        return new RecordWriter<IEtlKey, CamusWrapper>() {

            @Override
            public void write(IEtlKey iEtlKey, CamusWrapper camusWrapper) throws IOException, InterruptedException {
                
                TupleFactory tf = TupleFactory.getInstance();
                Tuple t = tf.newTuple();

                // TODO write tuple

                writer.write(t);
            }

            @Override
            public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
                writer.close();
            }
        };
    }
}
