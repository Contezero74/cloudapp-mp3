import java.io.BufferedReader;
import java.io.Closeable;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.File;
import java.io.IOException;
import java.util.Map;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileReaderSpout implements IRichSpout {
  private final static Logger LOG = LoggerFactory.getLogger(FileReaderSpout.class);

  private final String filepath;
  private boolean isCompleted = false;

  private SpoutOutputCollector collector;
  private TopologyContext context;

  public FileReaderSpout(final String filepath) {
    this.filepath = filepath;
  }

  @Override
  public void open(Map conf,
                   TopologyContext context,
                   SpoutOutputCollector collector) {
    this.context = context;
    this.collector = collector;
  }

  @Override
  public void nextTuple() {
    if (!isCompleted) {
      Closeable closeable = null;
      try {
        // both the FileReader and BufferedReader instances are directly
        // created and initialized here because they don't support
        // Serialization.
        final FileReader fileReader = new FileReader(filepath);
        closeable = fileReader;
        
        final BufferedReader reader = new BufferedReader(fileReader);
        closeable = reader;
        
        String line;
        while ( null != (line = reader.readLine()) ) {
          collector.emit( new Values(line) );
        }
        
        LOG.info("The file {} is finished", filepath);
      } catch (IOException e) {
        LOG.error("Generic exception during tuple retrieving: {}", e);
      } finally {
        isCompleted = true;
        
		    if (null != closeable) {
		      try {
		        closeable.close();
		      } catch (IOException e) {
		        LOG.error("Error during reader ({}) closing: {}", closeable, e);
		      }
        }
      }
    } else {
      Utils.sleep(100);
    }
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields("word"));
  }

  @Override
  public void close() {
    // nothing to do... all already closed
  }


  @Override
  public void activate() {
  }
  
  @Override
  public void deactivate() {
  }

  @Override
  public void ack(Object msgId) {
  }

  @Override
  public void fail(Object msgId) {
  }

  @Override
  public Map<String, Object> getComponentConfiguration() {
    return null;
  }
}
