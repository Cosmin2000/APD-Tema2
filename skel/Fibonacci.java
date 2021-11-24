public class Fibonacci {


	public Fibonacci() {

	}

	public static long fibo (long n) {
		if (n <= 1){
			return n;
		}

		long f1 = Fibonacci.fibo(n-1);
		long f2 = Fibonacci.fibo(n-2);
		return  f1 + f2;
	}
}
