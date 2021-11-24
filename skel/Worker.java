import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.RandomAccessFile;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static java.lang.Integer.max;
import static java.lang.Integer.parseInt;

public class Worker implements Callable<List<Object>> {
	String filename;
	int offset;
	int dim;
	AtomicInteger inQueue;
	ExecutorService tpe;

	public Worker(String filename, int offset, int dim, AtomicInteger inQueue, ExecutorService tpe) {
		this.filename = filename;
		this.offset = offset;
		this.dim = dim;
		this.inQueue = inQueue;
		this.tpe = tpe;
	}

	public boolean isDelimiter (char c) {
		if ((c >= 48 && c <= 57) || (c >= 65 && c <= 90) || (c >= 97 && c <= 122)) {
			return false;
		}
		return  true;
	}

	@Override
	public List<Object> call() throws Exception {
		// OPERATIA MAP
		String delims = ";:/?˜\\.,><‘[]{}()!@#$%ˆ&- +’=*”|\t\r\n ";
		File file = new File(filename);
		//instantiating the RandomAccessFile
		RandomAccessFile raFile = new RandomAccessFile(file, "r");
		//Reading each line using the readLine() method
		List<Character> line = new ArrayList<>();
		int i = 0;

		// VERIFIC DACA SE FRAGMENTUL SE INCEPE IN MIJLOCUL UNUI CUVANT
		boolean skipFirstWord = false;
		int ch,d = 0;
		if (offset > 0) {
			raFile.seek(offset - 1);
			if (((ch = raFile.read()) != -1) && ((d = raFile.read()) != -1) ) {
				if (!isDelimiter((char)ch) && !isDelimiter((char)d)) {
					skipFirstWord = true;
				}

			}

		}
		raFile.seek(offset);
		while(dim > 0) {
			line.add((char)raFile.read());
			dim--;
		}

		// VERIFIC DACA SE FRAGMENTUL SE INCHIDE IN MIJLOCUL UNUI CUVANT
		int c;
		if ((c = raFile.read()) != -1) {

			if (!isDelimiter(line.get(line.size() - 1)) && !isDelimiter((char)c)) {
				line.add((char)c);
				while (((c = raFile.read()) != -1) && !isDelimiter((char)c)) {
					line.add((char)c);
				}
			}
		}


		//System.out.println(line + " " +  skipFirstWord);
		ArrayList<String> words = new ArrayList<>();
		String str = line.stream().map(Object::toString)
				.collect(Collectors.joining(""));
		//System.out.println(str);
		StringTokenizer st = new StringTokenizer(str,delims);
		if (st.hasMoreTokens() && skipFirstWord) {
			st.nextToken();
		}
		while (st.hasMoreTokens()) {
			words.add(st.nextToken());

		}
		//System.out.println(words);
		// Dictionarul cerut
		Map<Integer, Long> dictionar = words.stream()
				.map(String::length)
				.collect(Collectors.groupingBy(t -> t, Collectors.counting()));

		//System.out.println(dictionar);
		Set<Integer> keys =  (dictionar.keySet());
		int max = 0;
		for (Integer key : keys) {
			if (key > max) {
				max = key;
			}
		}
		//System.out.println(max);
		//Integer b =Collections.max(keys);
		//System.out.println(b);
		int maxKey = max;
		List<String> maxWords = words.stream()
				.filter(t -> t.length() == maxKey)
				.collect(Collectors.toList());
		//System.out.println(maxWords);

		int left = inQueue.decrementAndGet();
		if (left == 0) {
			tpe.shutdown();
		}

		List<Object> returnMap = new ArrayList<>();
		returnMap.add(dictionar);
		returnMap.add(maxWords);
		return returnMap;

	}
}
