import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import static java.util.stream.Collectors.summingLong;

public class ReduceWorker implements Callable<List<Object>>  {
	String filename;
	List<Pair<Map<Integer, Long>, List<String>>> mapResult;
	ExecutorService executor;
	AtomicInteger inQueue;

	public ReduceWorker(String filename, List< Pair<Map<Integer, Long>, List<String>>>mapResult, ExecutorService executor, AtomicInteger inQueue) {
		this.filename = filename;
		this.mapResult = mapResult;
		this.executor = executor;
		this.inQueue = inQueue;
	}


	@Override
	public List<Object> call() {
		//ETAPA DE COMBINARE

		// Creez lista de dictionare pentru tot fisierul
		List<Map<Integer,Long>> maps =  mapResult.stream()
				.map(Pair::getA)
				.collect(Collectors.toList());

		// Creez un singur dictionar din lista de dictionare care are drept cheie,
		// numarul de aparitii, iar ca  valoare numarul de aparitii
		Map<Integer, Long> documentMap = maps.stream()
						.flatMap(m -> m.entrySet().stream())
						.collect( Collectors.groupingBy(Map.Entry::getKey, summingLong(Map.Entry::getValue)));

		int maxLen = Collections.max(documentMap.keySet());

		// Creez lista de cuvinte de lungime maxima pentru tot fisierul
		List<String> words =  mapResult.stream()
				.map(Pair::getB)
				.flatMap(Collection::stream)
				.filter(s -> s.length() == maxLen)
				.collect(Collectors.toList());


		//	ETAPA DE  PROCESARE

		// Pentru fiecare lungime, inmultesc valoarea din Sirul lui Fibonacci
		// cu numarul de aparitii si apoi adun aceste valori
		long numarator = documentMap.entrySet().stream()
				.map(t -> Fibonacci.fibo(t.getKey() + 1) * t.getValue() )
				.reduce(0L, Long::sum);


		// Numarul total de cuvinte din document
		long nr_cuv = documentMap.values().stream()
				.reduce(0L, Long::sum);

		// rang-ul documentului
		float rang = (float)numarator/nr_cuv;


	// daca e ultimul worker, opresc executorul.
		int left = inQueue.decrementAndGet();
		if (left == 0) {
			executor.shutdown();
		}

		// Creez rezulatul unui Task Reduce adica  numele fisierului, rang-ul
		// lungimea maxima si numarul de cuvinte de lungime maxima.

		List<Object> list = new ArrayList<>();
		list.add(filename);
		list.add(rang); // rang
		list.add(maxLen); // lungime maxima
		list.add(words.size()); // numarul de cuvinte de lungime maxima
		return list;
	}
}
