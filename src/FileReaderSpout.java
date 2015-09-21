
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Map;
import java.util.Scanner;
import java.lang.InterruptedException;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class FileReaderSpout implements IRichSpout {
  private final static Logger LOG = LoggerFactory.getLogger(FileReaderSpout.class);

  private transient final Scanner scanner;

  private SpoutOutputCollector _collector;
  private TopologyContext context;


  public FileReaderSpout(final String path) throws FileNotFoundException {
	try {
		scanner = new Scanner(new File(path));
	} catch (FileNotFoundException e) {
		LOG.error("the file '{}' doesn't exist", path);
		throw e;	
	}
  }

  @Override
  public void open(Map conf, TopologyContext context,
                   SpoutOutputCollector collector) {

     /*
    ----------------------TODO-----------------------
    Task: initialize the file reader


    ------------------------------------------------- */

    // nothing to do... all in the constructor....

    this.context = context;
    this._collector = collector;
  }

  @Override
  public void nextTuple() {

     /*
    ----------------------TODO-----------------------
    Task:
    1. read the next line and emit a tuple for it
    2. don't forget to sleep when the file is entirely read to prevent a busy-loop

    ------------------------------------------------- */

    if (scanner.hasNext()) {
	_collector.emit( new Values(scanner.nextLine()) );
    } {
	LOG.info("the file is finished");
	try {	
		Thread.sleep( 2 * 60 * 1000);
	} catch (InterruptedException e) {
		LOG.info("Spout nextTuple sleep interrupted");
	}	
    }
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {

    declarer.declare(new Fields("word"));

  }

  @Override
  public void close() {
   /*
    ----------------------TODO-----------------------
    Task: close the file


    ------------------------------------------------- */

    if (null != scanner) {
	scanner.close();
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
