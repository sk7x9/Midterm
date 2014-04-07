package consumer;
import org.apache.mahout.classifier.naivebayes.BayesUtils;
import org.apache.mahout.classifier.naivebayes.NaiveBayesPattern;
import org.apache.mahout.common.Pair;
import org.apache.mahout.common.iterator.sequencefile.SequenceFileIterable;


public class TopCompliants {
	
	public static Map<Integer, String> readInv_dict(Configure_path conf, Path Dict_path) {
		Map<Integer, String> inv_dict = new HashMap<Integer, String>();
		for (Pair<Text, IntWritable> pair : new SequenceFileIterable<Text, IntWritable>(Dict_path, true, conf)) {
			inv_dict.put(pair.getSecond().get(), pair.getFirst().toString());
		}
		return inv_dict;
	}
	
	public static Map<Integer, Long> readDoc_freq(Configure_path conf, Path doc_freqPath) {
		Map<Integer, Long> doc_freq = new HashMap<Integer, Long>();
		for (Pair<IntWritable, LongWritable> pair : new SequenceFileIterable<IntWritable, LongWritable>(doc_freqPath, true, conf)) {
			doc_freq.put(pair.getFirst().get(), pair.getSecond().get());
		}
		return doc_freq;
	}

	public static Map<Integer, Long> getcompliants(Map<Integer, Long> doc_freq, int topcompliantsCount) {
		List<Map.Entry<Integer, Long>> value = new ArrayList<Map.Entry<Integer, Long>>(doc_freq.entrySet());
		Collections.sort(value, new Comparator<Map.Entry<Integer, Long>>() {
			@Override
			public int compare(Entry<Integer, Long> e1, Entry<Integer, Long> e2) {
				return -e1.getValue().compareTo(e2.getValue());
			}
		});
		
		Map<Integer, Long> topcompliants = new HashMap<Integer, Long>();
		int k = 0;
		for(Map.Entry<Integer, Long> entry: value) {
			topcompliants.put(entry.getText_id(), entry.getValue());
			k++;
			if (k > topcompliantsCount) {
				break;
			}
		}
		
		return topcompliants;
	}
	
	public static class Compliant_impimplements Comparable<Compliant_imp> {
		private int compliant_id;
		private double imp;
		
		public Compliant_imp(int compliant_id, double imp) {
			this.compliant_id = compliant_id;
			this.imp = imp;
		}
		
		public int getCompliant_id() {
			return compliant_id;
		}

		public Double getImp() {
			return imp;
		}

		@Override
		public int compareTo(Compliant_imp) {
			return -getImp().compareTo(w.getImp());
		}
	}

	public static void main(String[] args) throws Exception {
		if (args.length < 4) {
						return;
		}
		String patterns_path = args[0];
		String label_path = args[1];
		String dict_path = args[2];
		String doc_freqPath = args[3];
		
		Configure_path configure_path = new Configure_path();

		// pattern is a matrix (compliant_id, labelId) => probability score
		NaiveBayesPattern pattern = NaiveBayesPattern.materialize(new Path(patterns_path), configure_path);
		
		// labels is a map label => classId
		Map<Integer, String> labels = BayesUtils.readLabelIndex(configure_path, new Path(label_path));
		Map<Integer, String> inv_dict = readInv_dict(configure_path, new Path(dict_path));
		Map<Integer, Long> doc_freq = readDoc_freq(configure_path, new Path(doc_freqPath));

		Map<Integer, Long> topcompliants = getcompliants(doc_freq, 10);
		
		for(Map.Entry<Integer, Long> entry: topcompliants.entrySet()) {
			
		}
		
		int count_lbl = labels.size();
		int compliantcount = doc_freq.get(-1).intValue();
		
		

		
		for(int labelId = 0 ; labelId < pattern.numLabels() ; labelId++) {
			SortedSet<Compliant_imp> compliant_imp = new TreeSet<Compliant_imp>();
			for(int compliant_id = 0 ; compliant_id < pattern.numFeatures() ; compliant_id++) {
				Compliant_imp = new Compliant_imp(compliant_id, pattern.imp(labelId, compliant_id));
				compliant_imp.add(w);
			}
						int j = 0;
			for(Compliant_imp: compliant_imp) {
				
				j++;
				if (j >= 10) {
					break;
				}
			}
		}
	}
}
