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
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import java.io.IOException;

public class CustomParquetOutputFormat <Void, V> extends DeprecatedParquetOutputFormat<V> {

        @Override
        public RecordWriter<java.lang.Void, V> getRecordWriter(FileSystem fs,
                                                               JobConf conf, String name, Progressable progress) throws IOException {
                return new CustomParquetOutputFormat.RecordWriterWrapper(realOutputFormat, fs, conf, name, progress);
        }

        //it's needed here as it's private in super class
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

        private CompressionCodecName getCodec(final JobConf conf) {
                return CodecConfig.from(conf).getCodec();
        }

        private static Path getDefaultWorkFile(JobConf conf, String name, String extension) {
                String file = getUniqueName(conf, name) + extension;
                return new Path(getWorkOutputPath(conf), file);
        }
}
