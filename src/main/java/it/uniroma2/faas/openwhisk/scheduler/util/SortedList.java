package it.uniroma2.faas.openwhisk.scheduler.util;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;

public class SortedList<T> extends ArrayList<T> {

    private final Comparator<? super T> comparator;

    public SortedList(int initialCapacity, Comparator<? super T> comparator) {
        super(initialCapacity);
        this.comparator = comparator;
    }

    public SortedList() {
        this(null);
    }

    public SortedList(Comparator<? super T> comparator) {
        this.comparator = comparator;
    }

    public SortedList(Collection<? extends T> c, Comparator<? super T> comparator) {
        super(c);
        this.comparator = comparator;
    }

    // see@ https://stackoverflow.com/questions/4903611/java-list-sorting-is-there-a-way-to-keep-a-list-permantly-sorted-automatically
    @Override
    public boolean add(final T t) {
        int index = indexedBinarySearch(this, t);
        if (index < 0) index = ~index;
        super.add(index, t);
        return true;
    }

    public Comparator<? super T> comparator() {
        return comparator;
    }

    private int indexedBinarySearch(@Nonnull final List<T> list, final T key) {
        int low = 0;
        int high = list.size() - 1;

        while (low <= high) {
            int mid = (low + high) >>> 1;
            final T midVal = list.get(mid);
            int cmp = compare(midVal, key);

            if (cmp < 0) low = mid + 1;
            else if (cmp > 0) high = mid - 1;
            // key found
            else return mid;
        }

        // key not found
        return -(low + 1);
    }

    /**
     * Compares two items using the correct comparison method for this SortedList.
     */
    @SuppressWarnings("unchecked")
    private int compare(Object o1, Object o2) {
        return comparator == null
                ? ((Comparable<? super  T>) o1).compareTo((T) o2)
                : comparator.compare((T) o1, (T) o2);
    }

}