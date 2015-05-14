package net.redborder.camus;


import com.linkedin.camus.coders.Partitioner;
import com.linkedin.camus.etl.IEtlKey;
import com.linkedin.camus.etl.kafka.common.DateUtils;
import com.linkedin.camus.etl.kafka.mapred.EtlMultiOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.JobContext;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormatter;

public class RbPartitioner extends Partitioner{

    protected static final String OUTPUT_DATE_FORMAT = "YYYY/MM/dd/HH";
    protected DateTimeFormatter outputDateFormatter = null;

    @Override
    public String encodePartition(JobContext context, IEtlKey key) {
        long outfilePartitionMs = EtlMultiOutputFormat.getEtlOutputFileTimePartitionMins(context) * 60000L;
        return ""+DateUtils.getPartition(outfilePartitionMs, key.getTime(), outputDateFormatter.getZone())+"=="+key.getService();
    }

    @Override
    public String generatePartitionedPath(JobContext context, String topic, String brokerId, int partitionId, String encodedPartition) {
        String [] keys = encodedPartition.split("==");
        StringBuilder sb = new StringBuilder();
        sb.append(keys[1]).append("/");
        sb.append(topic).append("/");
        sb.append(EtlMultiOutputFormat.getDestPathTopicSubDir(context)).append("/");
        DateTime bucket = new DateTime(Long.valueOf(keys[0]));
        sb.append(bucket.toString(outputDateFormatter));
        return sb.toString();    }

    @Override
    public void setConf(Configuration conf)
    {
        if (conf != null){
            outputDateFormatter = DateUtils.getDateTimeFormatter(OUTPUT_DATE_FORMAT, DateTimeZone.forID("UTC"));
        }

        super.setConf(conf);
    }
}
