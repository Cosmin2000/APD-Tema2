
import java.io.File;
import java.io.RandomAccessFile;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;


public class MapWorker implements Callable<Triplet<Map<Integer, Long>,List<String>, String>>  {
	String filename;
	int offset;
	int dim;
	AtomicInteger inQueue;
	ExecutorService tpe;

	public MapWorker(String filename, int offset, int dim, AtomicInteger inQueue, ExecutorService tpe) {
		this.filename = filename;
		this.offset = offset;
		this.dim = dim;
		this.inQueue = inQueue;
		this.tpe = tpe;
	}

	public boolean isDelimiter (char c) {
		return (c < 48 || c > 57) && (c < 65 || c > 90) && (c < 97 || c > 122);
	}

	@Override
	public Triplet<Map<Integer, Long>, List<String>, String> call() throws Exception {
		// OPERATIA MAP
		String delims = ";:/?˜\\.,><'[]{}()!@#$%ˆ&-_+’=*”`|\t\r\n ";
		// Deschid fisierul pentru citire
		File file = new File(filename);
		RandomAccessFile raFile = new RandomAccessFile(file, "r");
		List<Character> chars = new ArrayList<>();

		// Verific daca fragmentul incepe in interiorul unui cuvant
		boolean skipFirstWord = false;
		int ch,d ;
		if (offset > 0) { // verific doar daca nu e primul fragment din fisier
			raFile.seek(offset - 1);
			// citesc caracter-ul de dinaintea offset-ului, dar si caracter-ul de unde trebuia sa incep
			if (((ch = raFile.read()) != -1) && ((d = raFile.read()) != -1) ) {
				if (!isDelimiter((char)ch) && !isDelimiter((char)d)) {
					skipFirstWord = true;
				}
				// daca nu sunt delimitatori, trebuie sa sar peste primul cuvant
			}

		}
		raFile.seek(offset);
		// citesc dimensiunea caracter cu caracter
		while(dim > 0) {
			chars.add((char)raFile.read());
			dim--;
		}


		// Verific daca fragmentul se inchide in mijlocul unui cuvant
		int c;
		// Mai citesc un char in plus daca se poate (daca nu e ultimul fragment)
		// Daca nu e delimitator, citesc pana intalnesc un delimitator
		if ((c = raFile.read()) != -1) {

			if (!isDelimiter(chars.get(chars.size() - 1)) && !isDelimiter((char)c)) {
				chars.add((char)c);
				while (((c = raFile.read()) != -1) && !isDelimiter((char)c)) {
					chars.add((char)c);
				}
			}
		}

		raFile.close();
		// lista de cuvinte
		ArrayList<String> words = new ArrayList<>();

		// creez un string din lista de caractere
		String str = chars.stream().map(Object::toString)
				.collect(Collectors.joining(""));

		// Separ cuvintele.
		// Daca fragmentul incepe in interiorul unui cuvant, sar peste primul.
		StringTokenizer st = new StringTokenizer(str,delims);
		if (st.hasMoreTokens() && skipFirstWord) {
			st.nextToken();
		}
		while (st.hasMoreTokens()) {
			words.add(st.nextToken());

		}

		// Creez dictionarul care are drept cheie, lungimea cuvantului,
		// iar ca valoare numarul de cuvinte cu acea lungime.
		Map<Integer, Long> dictionar = words.stream()
				.map(String::length)
				.collect(Collectors.groupingBy(t -> t, Collectors.counting()));

		Set<Integer> keys =  dictionar.keySet();

		// Calculez lungimea maxima
		int max = 0;
		for (Integer key : keys) {
			if (key > max) {
				max = key;
			}
		}

		int maxLen = max;

		// Creez lista de cuvinte de lungime maxima, filtrand lista de cuvinte
		// si pastrand cuvintele care au lungime  egala cu cea maxima
		List<String> maxWords = words.stream()
				.filter(t -> t.length() == maxLen)
				.collect(Collectors.toList());

		// daca e ultimul worker, opresc executorul.
		int left = inQueue.decrementAndGet();
		if (left == 0) {
			tpe.shutdown();
		}

		return new Triplet<>(dictionar, maxWords, filename);

	}
}
