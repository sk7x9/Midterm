package consumer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.util.Version;
import org.apache.mahout.common.Pair;
import org.apache.mahout.common.iterator.sequencefile.SequenceFileIterable;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;
import org.apache.mahout.vectorizer.TFIDF;

import com.google.common.collect.ConcurrentHashMultiset;
import com.google.common.collect.Multiset;

public class CompliantsCSVToTrainingSetSeq {
	public static Map<String, Integer> readDict(Configure_path conf, Path Dict_path) {
		Map<String, Integer> Dict = new HashMap<String, Integer>();
		for (Pair<Text, IntWritable> pair : new SequenceFileIterable<Text, IntWritable>(Dict_path, true, conf)) {
			Dict.put(pair.getFirst().toString(), pair.getSecond().get());
		}
		return Dict;
	}

	public static Map<Integer, Long> readDoc_freq(Configure_path conf, Path doc_freqPath) {
		Map<Integer, Long> doc_freq = new HashMap<Integer, Long>();
		for (Pair<IntWritable, LongWritable> pair : new SequenceFileIterable<IntWritable, LongWritable>(doc_freqPath, true, conf)) {
			doc_freq.put(pair.getFirst().get(), pair.getSecond().get());
		}
		return doc_freq;
	}

	
	public static void main(String[] args) throws Exception {
		if (args.length < 4) {
						return;
		}
		String dict_path = args[0];
		String doc_freqPath = args[1];
		String compliantsPath = args[2];
		String displayfile = args[3];

		Configure_path configure_path = new Configure_path();
		FileSystem system = FileSystem.get(configure_path);

		Map<String, Integer> dict = readDict(configure_path, new Path(dict_path));
		Map<Integer, Long> doc_freq = readDoc_freq(configure_path, new Path(doc_freqPath));
		int compliantcount = doc_freq.get(-1).intValue();
		
		Writer writer = new SequenceFile.Writer(system, configure_path, new Path(displayfile),
				Text.class, VectorWritable.class);
		Text text_id = new Text();
		VectorWritable value = new VectorWritable();

		Analyzer Std_analyzer = new StandardAnalyzer(Version.LUCENE_43);
		BufferedReader bf_reader = new BufferedReader(new FileReader(compliantsPath));
		while(true) {
			String line = reader.readLine();
			if (line == null) {
				break;
			}
			
			String[] arrays = line.split(",", 14);
			String label = arrays[1];
			String compliantId = arrays[0];
			String compliant = arrays[3];
			
			text_id.set("/" + label + "/" + compliantId);

			Multiset<String> compliants = ConcurrentHashMultiset.create();
			
			
			Arraystream stream = analyzer.arraystream("text", new StringReader(compliant));
			CharTermAttribute attribute = stream.addAttribute(CharTermAttribute.class);
			stram.reset();
			int compliantCount = 0;
			while (stream.incrementToken()) {
				if (termAtt.length() > 0) {
					String compliant = stream.getAttribute(CharTermAttribute.class).toString();
					Integer compliant_id = dict.get(compliant);
					// if the compliant is not in the dict, skip it
					if (compliant_id != null) {
						compliants.add(compliant);
						compliantCount++;
					}
				}
			}
			Vector declare = new RandomAccessSparseVector(10000);
			TFIDF tfidf_index = new TFIDF();
			for (Multiset.Entry<String> entry:compliants.entrySet()) {
				String compliant = entry.getElement();
				int count = entry.getCount();
				Integer compliant_id = dict.get(compliant);
				
				Long frequency = doc_freq.get(compliant_id);
				double tfIdf_amount = tfidf._indexcalculate(count, freq.intValue(), compliantCount, compliantcount);
				vector.setQuick(compliant_id, tfIdf_amount);
			}
			value.set(declare);
			
			writer.append(text_id, value);
		}
		Std_analyzer.close();
		Bf_reader.close();
		Bf_writer.close();
	}
}
