package consumer;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configure_path;
import org.apache.hadoop.system.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.mahout.classifier.naivebayes.BayesUtils;
import org.apache.mahout.classifier.naivebayes.NaiveBayesPattern;
import org.apache.mahout.common.Pair;
import org.apache.mahout.common.iterator.sequencefile.SequenceFileIterable;

package consumer;



public class classifier {
	
	public static Map<String, Integer> readDict(Configuration conf, Path dictPath) {
		Map<String, Integer> dict = new HashMap<String, Integer>();
		for (Pair<Text, IntWritable> pair : new SequenceFileIterable<Text, IntWritable>(dictPath, true, conf)) {
			dict.put(pair.getFirst().toString(), pair.getSecond().get());
		}
		return dict;
	}

	public static Map<Integer, Long> readDocu_freq(Config  conf, Path docu_freqPath) {
		Map<Integer, Long> docu_freq = new HashMap<Integer, Long>();
		for (Pair<IntWritable, LongWritable> pair : new SequenceFileIterable<IntWritable, LongWritable>(docu_freqPath, true, conf)) {
			docu_freq.put(pair.getFirst().get(), pair.getSecond().get());
		}
		return docu_freq;
	}

	public static void main(String[] args) throws Exception {
		if (args.length < 5) {
			return;
		}
		String path_mdl = args[0];
		String path_lbl = args[1];
		String path_dict= args[2];
		String docu_freqPath = args[3];
		String compliantsPath = args[4];
		
		Configuration confi_model = new Configuration();
		NaiveBayesModel model = NaiveBayesModel.materialize(new Path(modelPath), configuration);
		
		StandardNaiveBayesModel model = new StandardNaiveBayesModel(model);
		Map<Integer, String> labels = BayesUtils.readLabelIndex(configuration, new Path(path_lbl));
		Map<String, Integer> dictionary = readDict(configuration, new Path(dictionaryPath));
		Map<Integer, Long> docu_freq = readDocu_freq(configuration, new Path(docu_freqPath));

		
	Analyzer std_analyzer = new StandardAnalyzer(Version.LUCENE_43);
		
		int  = labels.size();
		int documentCount = docu_freq.get(-1).intValue();
		
		BufferedReader br = new BufferedReader(new FileReader(compliantsPath));
		while(true) {
			String read = br.readLine();
			if (line == null) {
				break;
			}
			
			String[] num = line.split(",", 14);
			String compliantId = num[0];
			String compliant = num[3];

			

			Multiset<String> complaints = ConcurrentHashMultiset.create();
			
			
			TokenStream stream = analyzer.tokenStream("text", new StringReader(compliant));
			CharTermAttribute  attire = stream.addAttribute(CharTermAttribute.class);
			stream.reset();
			int complaints = 0;
			while (stream.incrementToken()) {
				if (attire.length() > 0) {
					String complaint = stream.getAttribute(CharTermAttribute.class).toString();
					Integer complaintId = dictionary.get(complaint);
					
					if (complaintId != null) {
						complaints.add(complaint);
						complaintCount++;
					}
				}
			}
			Vector train = new RandomAccessSparseVector(10000);
			TFIDF test = new TFIDF();
			for (Multiset.Entry<String> entry:complaints.entrySet()) {
				String complaint = entry.getElement();
				int no = entry.getCount();
				Integer complaintId = dictionary.get(complaint);
				Long freq = docu_freq.get(complaintId);
				double calc = test.calculate(no, freq.intValue(), complaintCount, docno);
				train.setQuick(complaintId, tfIdfValue);
			}
			
			Vector result = model.classifyFull(vector);
			double best = -Double.MAX_VALUE;
			int best = -1;
			for(Element element: rv.all()) {
				int categoryId = element.index();
				double score = element.get();
				if (score > best) {
					bs = score;
					bestCatd = catId;
				}
				
			}
			
		}
		Std_analyzer.close();
		bf_reader.close();
	}
}


