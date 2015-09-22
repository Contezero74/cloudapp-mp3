
import java.io.BufferedReader;
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

  private FileReader fileReader;
  private BufferedReader reader;
  private boolean isCompleted = false;

  private SpoutOutputCollector collector;
  private TopologyContext context;


  public FileReaderSpout(final String filepath) throws FileNotFoundException {
	this.filepath = filepath;
	try {
		this.fileReader = new FileReader(filepath);
	} catch (FileNotFoundException e) {
		LOG.error("The file {} doesn't exist", filepath);
		throw e;
	}
  }

  @Override
  public void open(Map conf,
		   TopologyContext context,
                   SpoutOutputCollector collector) {

    this.reader = new BufferedReader(fileReader);
    this.context = context;
    this.collector = collector;
  }

  @Override
  public void nextTuple() {
    if (!isCompleted) {
	try {
		final String line = reader.readLine();
		if (null != line) {
			collector.emit( new Values(line) );
		} else {
			LOG.info("The file {} is finished", filepath);
			isCompleted = true;
		}
	} catch (IOException e) {
		LOG.error("Generic exception during tuple retrieving: {}", e);
	} finally {
		isCompleted = true;
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
    try {
	    if (null != reader) {
		reader.close();
	    } else if (null != fileReader) {
		fileReader.close();
	    }
    } catch (IOException e) {
	LOG.error("Generic exception during spout closing: {}", e);
    }
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
