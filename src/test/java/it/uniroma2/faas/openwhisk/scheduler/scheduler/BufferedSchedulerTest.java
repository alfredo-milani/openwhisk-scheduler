package it.uniroma2.faas.openwhisk.scheduler.scheduler;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class BufferedSchedulerTest {

    @Test
    public void pairwise() {
        assertEquals(new ArrayList<>(), pairwiseCoprimeNumbersUntil(0));
        assertEquals(new ArrayList<>(), pairwiseCoprimeNumbersUntil(-1));

        assertEquals(new ArrayList<>() {{ add(1); }}, pairwiseCoprimeNumbersUntil(1));
        assertEquals(new ArrayList<>() {{ add(1); }}, pairwiseCoprimeNumbersUntil(2));
        assertEquals(new ArrayList<>() {{ add(1); add(2); }}, pairwiseCoprimeNumbersUntil(3));
        assertEquals(new ArrayList<>() {{ add(1); add(3); }}, pairwiseCoprimeNumbersUntil(4));
        assertEquals(new ArrayList<>() {{ add(1); add(2); add(3); }}, pairwiseCoprimeNumbersUntil(5));
        assertEquals(new ArrayList<>() {{ add(1); add(2); add(5); add(7); }}, pairwiseCoprimeNumbersUntil(9));
        assertEquals(new ArrayList<>() {{ add(1); add(3); add(7); }}, pairwiseCoprimeNumbersUntil(10));
    }

    private static int gcd(int a, int b) {
        if (b == 0) return a;
        else return gcd(b, a % b);
    }

    private static List<Integer> pairwiseCoprimeNumbersUntil(int n) {
        final ArrayList<Integer> primes = new ArrayList<>();
        IntStream.rangeClosed(1, n).forEach(i -> {
            if (gcd(i, n) == 1 && primes.stream().allMatch(j -> gcd(j, i) == 1)) primes.add(i);
        });
        return primes;
    }

}