package jobs;

/** Shared prime-number helper used by PRIMESUM and PRIMECOUNT. */
public final class PrimeUtils {

    private PrimeUtils() {}

    public static boolean isPrime(long n) {
        if (n < 2) return false;
        if (n < 4) return true;
        if (n % 2 == 0) return false;
        for (long i = 3; i * i <= n; i += 2) {
            if (n % i == 0) return false;
        }
        return true;
    }
}