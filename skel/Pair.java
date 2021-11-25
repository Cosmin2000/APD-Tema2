

public class Pair<K,V> {
	K a;
	V b;

	public K getA() {
		return a;
	}

	public void setA(K a) {
		this.a = a;
	}

	public V getB() {
		return b;
	}

	public void setB(V b) {
		this.b = b;
	}

	public Pair(K a, V b) {
		this.a = a;
		this.b = b;
	}

	public Pair() {
	}
}
