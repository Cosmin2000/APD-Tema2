import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.stream.Collectors.summingInt;

public class Reduce implements Callable<Map<String, List<Object>>>  {
	String filename;
	List<List<Object>> mapResult;
	ExecutorService executor;
	AtomicInteger inQueue;

	public Reduce(String filename, List<List<Object>>mapResult, ExecutorService executor, AtomicInteger inQueue) {
		this.filename = filename;
		this.mapResult = mapResult;
		this.executor = executor;
		this.inQueue = inQueue;
	}


	@Override
	public Map<String, List<Object>> call() {
		//ETAPA DE COMBINARE

		//mapResult.forEach(t -> System.out.println(t));
		List<Map<Integer,Integer>> maps =  mapResult.stream()
				.map(t -> (Map<Integer,Integer>)t.get(0))
				.collect(Collectors.toList());
		Map<Integer, Integer> documentMap = maps.stream()
						.flatMap(m -> m.entrySet().stream())
						.collect( Collectors.groupingBy(Map.Entry::getKey, summingInt(Map.Entry::getValue)));
		//System.out.println(documentMap);

		int max = Collections.max(documentMap.keySet());
		List<String> words =  mapResult.stream()
				.map(t-> (List<String>)t.get(1))
				.flatMap(Collection::stream)
				.filter(s -> s.length() == max)
				.collect(Collectors.toList());

		//System.out.println(max);
		//System.out.println(words);

		//	ETAPA DE  PROCESARE

		long numitor = documentMap.entrySet().stream()
				.map(t -> Fibonacci.fibo(t.getKey() + 1) * t.getValue() )
				.reduce(0L, Long::sum);
		//System.out.println(numitor);

		long nr_cuv = documentMap.values().stream()
				.reduce(0, Integer::sum);
		float rang = (float)numitor/nr_cuv;
		//System.out.println(filename+ ", " +String.format("%.2f", rang) + ", " + max + ", " + words.size());


		int left = inQueue.decrementAndGet();
		if (left == 0) {
			executor.shutdown();
		}

		Map<String,List<Object>> retMap = new HashMap<>();
		List<Object> list = new ArrayList<>();
		list.add(rang);
		list.add(max);
		list.add(words.size());
		retMap.put(filename,list);
		return retMap;
	}
}
