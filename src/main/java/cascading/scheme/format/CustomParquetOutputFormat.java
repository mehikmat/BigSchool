package cascading.scheme.format;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Progressable;
import org.apache.parquet.hadoop.ParquetOutputFormat;
import org.apache.parquet.hadoop.ParquetRecordWriter;
import org.apache.parquet.hadoop.codec.CodecConfig;
import org.apache.parquet.hadoop.mapred.DeprecatedParquetOutputFormat;
import org.apache.parquet.hadoop.mapred.MapredParquetOutputCommitter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import java.io.IOException;

public class CustomParquetOutputFormat<Void, V> extends org.apache.hadoop.mapred.FileOutputFormat<java.lang.Void, V> {

        protected ParquetOutputFormat<V> realOutputFormat = new ParquetOutputFormat<V>();

        public static void setWriteSupportClass(Configuration configuration, Class<?> writeSupportClass) {
                configuration.set(ParquetOutputFormat.WRITE_SUPPORT_CLASS, writeSupportClass.getName());
        }

        public static void setBlockSize(Configuration configuration, int blockSize) {
                configuration.setInt(ParquetOutputFormat.BLOCK_SIZE, blockSize);
        }

        public static void setPageSize(Configuration configuration, int pageSize) {
                configuration.setInt(ParquetOutputFormat.PAGE_SIZE, pageSize);
        }

        public static void setCompression(Configuration configuration, CompressionCodecName compression) {
                configuration.set(ParquetOutputFormat.COMPRESSION, compression.name());
        }

        public static void setEnableDictionary(Configuration configuration, boolean enableDictionary) {
                configuration.setBoolean(ParquetOutputFormat.ENABLE_DICTIONARY, enableDictionary);
        }

        public static void setAsOutputFormat(JobConf jobConf) {
                jobConf.setOutputFormat(DeprecatedParquetOutputFormat.class);
                jobConf.setOutputCommitter(MapredParquetOutputCommitter.class);
        }

        private static Path getDefaultWorkFile(JobConf conf, String name, String extension) {
                String file = getUniqueName(conf, name) + extension;
                return new Path(getWorkOutputPath(conf), file);
        }

        private CompressionCodecName getCodec(final JobConf conf) {
                return CodecConfig.from(conf).getCodec();
        }

        @Override
        public RecordWriter<java.lang.Void, V> getRecordWriter(FileSystem fs,
                                                               JobConf conf, String name, Progressable progress) throws IOException {
                return new CustomParquetOutputFormat.RecordWriterWrapper(realOutputFormat, fs, conf, name, progress);
        }

        private class RecordWriterWrapper implements RecordWriter<java.lang.Void, V> {
                private boolean recordExists = false;
                private Path file;

                private ParquetRecordWriter<V> realWriter;

                public RecordWriterWrapper(ParquetOutputFormat<V> realOutputFormat,
                                           FileSystem fs, JobConf conf, String name, Progressable progress) throws IOException {

                        CompressionCodecName codec = getCodec(conf);
                        String extension = codec.getExtension() + ".parquet";
                        file = getDefaultWorkFile(conf, name, extension);

                        try {
                                realWriter = (ParquetRecordWriter<V>) realOutputFormat.getRecordWriter(conf, file, codec);
                        } catch (InterruptedException e) {
                                Thread.interrupted();
                                throw new IOException(e);
                        }
                }

                @Override
                public void close(Reporter reporter) throws IOException {
                        try {
                                realWriter.close(null);

                                //delete empty parts
                                FileSystem fileSystem = file.getFileSystem(new Configuration());
                                if (!recordExists) {
                                        fileSystem.delete(file, true);
                                        System.out.println("Deleted empty part: " + file.toString());
                                }
                        } catch (InterruptedException e) {
                                Thread.interrupted();
                                throw new IOException(e);
                        }
                }

                @Override
                public void write(java.lang.Void key, V value) throws IOException {
                        try {
                                realWriter.write(key, value);
                                recordExists = true;
                        } catch (InterruptedException e) {
                                Thread.interrupted();
                                throw new IOException(e);
                        }
                }
        }
}
