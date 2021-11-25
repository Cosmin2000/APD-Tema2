import java.util.StringTokenizer;

public class Document implements Comparable<Document>{
	String name;
	int order;
	float rang;
	int max;
	int nr_words;

	@Override
	public int compareTo(Document o) {
		if (this.rang == o.rang) {
			return this.order - o.order;
		}
		if (this.rang > o.rang){
			return -1;
		}
		return  1;

	}

	public Document(String name, int order, float rang) {
		this.name = name;
		this.order = order;
		this.rang = rang;
	}

	public Document(String name, int order) {
		this.name = name;
		this.order = order;
	}

	public Document(String name) {
		this.name = name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public void setOrder(int order) {
		this.order = order;
	}

	public void setRang(float rang) {
		this.rang = rang;
	}

	public String getName() {
		return name;
	}

	public int getOrder() {
		return order;
	}

	public float getRang() {
		return rang;
	}

	public int getMax() {
		return max;
	}

	public void setMax(int max) {
		this.max = max;
	}

	public int getNr_words() {
		return nr_words;
	}

	public void setNr_words(int nr_words) {
		this.nr_words = nr_words;
	}

	@Override
	public String toString() {
		StringTokenizer tokenizer = new StringTokenizer(name,"/");
		String token = null;
		while (tokenizer.hasMoreTokens()) {
			token = tokenizer.nextToken();
		}
 		return token +","+ String.format("%.2f",rang) +"," + max +"," + nr_words;
	}
}
