package org.chenyou.fuzzdb.util;

/**
 * Created by ChenYou on 2018/1/1.
 */
public class Random {
    private int seed;
    public Random(int s) {
        this.seed = s & 0x7fffffff;
        // avoid bad seed
        if (this.seed == 0 || this.seed == 2147483647L) {
            this.seed = 1;
        }
    }
    public int next() {
        int M = 2147483647;
        long A = 16807L;
        long product = this.seed * A;
        long ski = (product >>> 31) + (product & M);
        this.seed = (int)((product >>> 31) + (product & M));
        if (Integer.compareUnsigned(this.seed, M) == 1) {
            long seedl = this.seed & 0xffffffffL;
            long ml = M & 0xffffffffL;
            this.seed = (int)(seedl - ml);
        }
        return this.seed;
    }

    // Returns a uniformly distributed value in the range [0..n-1]
    // REQUIRES: n > 0
    public int uniform(int n) {
        return next() % n;
    }

    // Randomly returns true ~"1/n" of the time, and false otherwise.
    // REQUIRES: n > 0
    public boolean oneIn(int n) {
        return (next() % n) == 0;
    }

    // Skewed: pick "base" uniformly from range [0,max_log] and then
    // return "base" random bits.  The effect is to pick a number in the
    // range [0,2^max_log-1] with exponential bias towards smaller numbers.
    public int skewed(int maxLog) {
        return uniform(1 << uniform(maxLog + 1));
    }
}
