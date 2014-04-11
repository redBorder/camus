package com.liquidm.camus;

import com.linkedin.camus.coders.CamusWrapper;
import com.linkedin.camus.etl.IEtlKey;
import com.linkedin.camus.etl.RecordWriterProvider;
import com.linkedin.camus.etl.kafka.mapred.EtlMultiOutputFormat;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import parquet.hadoop.ParquetWriter;
import parquet.hadoop.metadata.CompressionCodecName;
import parquet.proto.ProtoWriteSupport;

import java.io.IOException;

import static com.liquidm.Events.*;

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
        ProtoWriteSupport support = new ProtoWriteSupport(EventLogged.class);

        Path cwd = committer.getWorkPath();
        Path file = new Path(cwd, EtlMultiOutputFormat.getUniqueFile(context, fileName, getFilenameExtension()));

        final ParquetWriter writer = new ParquetWriter(file, support, CompressionCodecName.GZIP,  ParquetWriter.DEFAULT_BLOCK_SIZE, ParquetWriter.DEFAULT_PAGE_SIZE, false, false);

        return new RecordWriter<IEtlKey, CamusWrapper>() {


            @Override
            public void write(IEtlKey iEtlKey, CamusWrapper camusWrapper) throws IOException, InterruptedException {
                writer.write(camusWrapper.getRecord());
            }

            @Override
            public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
                writer.close();
            }
        };
    }
}
