package com.stock.engine.component;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicInteger;

public class OrderBookEngine {

    private Map<Double, AtomicInteger>  buyOrders      = new ConcurrentSkipListMap<>(
            Comparator.reverseOrder());

    private Map<Double, AtomicInteger>  sellOrders     = new ConcurrentSkipListMap<>();

    private List<Pair<Double, Integer>> executedOrders = new ArrayList<>();

    public int getExecutedOrdersCount() {
        return executedOrders.size();
    }

    public void printExecutedBook() {
        System.out.println("");
        System.out.println("____ exec ____ ");
        for (Pair<Double, Integer> pair : executedOrders) {
            System.out.println(pair.getRight() + " @ " + pair.getLeft());
        }
        System.out.println("______________ ");
    }

    public void printOrderBook() {

        System.out.println("");
        Set<Double> bid_prices = buyOrders.keySet();
        List<Double> bid_prices_list = new ArrayList<>(bid_prices);
        System.out.println("____ bid ____ ");
        for (Double bid_price : bid_prices_list) {
            System.out.println(buyOrders.get(bid_price) + " @ " + bid_price);
        }

        Set<Double> ask_prices = sellOrders.keySet();
        System.out.println("____ ask ____ ");
        for (double ask_price : ask_prices) {
            System.out.println(sellOrders.get(ask_price) + " @ " + ask_price);
        }
        System.out.println("_____________ ");

    }

    public void receiveOrder(double price, int quantity, boolean buy) {
        if (buy) {
            // BUY
            Iterator<Double> ask_prices = sellOrders.keySet().iterator();
            while (ask_prices.hasNext()) {
                Double ask_price = ask_prices.next();
                if (quantity > 0 && price >= ask_price) {
                    AtomicInteger ask_quantity = sellOrders.get(ask_price);
                    if (Objects.isNull(ask_quantity)
                            || (Objects.nonNull(ask_quantity) && ask_quantity.get() == 0)) {
                        continue;
                    }
                    Integer originalAskQuantity = ask_quantity.get();
                    if (quantity >= originalAskQuantity && removeSellOrder(ask_price, ask_quantity,
                            originalAskQuantity, quantity)) {
                        quantity = quantity - originalAskQuantity;
                        executedOrders.add(new Pair<>(ask_price, originalAskQuantity));
                    } else if (removeSellOrder(ask_price, ask_quantity, originalAskQuantity,
                            quantity)) {
                        executedOrders.add(new Pair<>(ask_price, quantity));
                        quantity = 0;
                    }
                }
            }
            if (quantity > 0) {
                addBuyOrder(price, quantity);
            }
        } else {
            Iterator<Double> bid_prices = buyOrders.keySet().iterator();
            while (bid_prices.hasNext()) {
                Double bid_price = bid_prices.next();
                if (quantity > 0 && price <= bid_price) {
                    AtomicInteger bid_quantity = buyOrders.get(bid_price);
                    if (Objects.isNull(bid_quantity)
                            || (Objects.nonNull(bid_quantity) && bid_quantity.get() == 0)) {
                        continue;
                    }
                    Integer originalBidQuantity = bid_quantity.get();
                    if (quantity >= originalBidQuantity
                            && removeBuyOrder(bid_price, bid_quantity, originalBidQuantity, quantity)) {
                        quantity = quantity - originalBidQuantity;
                        executedOrders.add(new Pair<>(bid_price, originalBidQuantity));
                    } else if (removeBuyOrder(bid_price, bid_quantity, originalBidQuantity,
                            quantity)) {
                        executedOrders.add(new Pair<>(bid_price, quantity));
                        quantity = 0;
                    }
                }
            }
            if (quantity > 0) {
                addSellOffer(price, quantity);
            }
        }
    }

    public void addBuyOrder(double price, int quantity) {
        if (Objects.isNull(buyOrders.computeIfPresent(price, (k, oldValue) -> {
            oldValue.getAndAdd(quantity);
            return oldValue;
        }))) {
            buyOrders.putIfAbsent(price, new AtomicInteger(quantity));
        }
    }

    public void addSellOffer(double price, int quantity) {
        if (Objects.isNull(sellOrders.computeIfPresent(price, (k, oldValue) -> {
            oldValue.getAndAdd(quantity);
            return oldValue;
        }))) {
            sellOrders.putIfAbsent(price, new AtomicInteger(quantity));
        }
    }

    public boolean removeBuyOrder(double price, AtomicInteger buyQuantity, int originalQuantity,
                                  int quantity) {
        if (originalQuantity <= quantity) {
            if (buyQuantity.compareAndSet(originalQuantity, 0)) {
                return buyOrders.remove(price, buyQuantity);
            }
            return false;
        } else {
            return buyQuantity.compareAndSet(originalQuantity, originalQuantity - quantity);
        }
    }

    public boolean removeSellOrder(double price, AtomicInteger sellQuantity, int originalQuantity,
                                   int quantity) {
        if (originalQuantity <= quantity) {
            if (sellQuantity.compareAndSet(originalQuantity, 0)) {
                return sellOrders.remove(price, sellQuantity);
            }
            return false;
        } else {
            return sellQuantity.compareAndSet(originalQuantity, originalQuantity - quantity);
        }
    }

    public int getAskLevel() {
        return sellOrders.size();
    }

    public int getBidLevel() {
        return buyOrders.size();
    }

    public int getBidQuantity(double bestPrice) {
        int bidQuantity = 0;
        for (double price : buyOrders.keySet()) {
            if (price > bestPrice) {
                bidQuantity += buyOrders.get(price).get();
            }
        }

        return bidQuantity;
    }

    public int getBidQuantity() {
        return getBidQuantity(Integer.MIN_VALUE);
    }

    public int getAskQuantity() {
        return getAskQuantity(Integer.MAX_VALUE);
    }

    public int getAskQuantity(double bestPrice) {
        int askQuantity = 0;
        for (double price : sellOrders.keySet()) {
            if (price < bestPrice) {
                askQuantity += sellOrders.get(price).get();
            }
        }
        return askQuantity;
    }

    public void reset() {
        System.out.println("size ask = " + sellOrders.size());
        System.out.println("size bid = " + buyOrders.size());
        System.out.println("executed orders = " + executedOrders.size());
        sellOrders.clear();
        buyOrders.clear();
        executedOrders.clear();
    }

}
