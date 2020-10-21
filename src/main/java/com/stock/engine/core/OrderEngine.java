package com.stock.engine.core;

import com.stock.engine.component.AbstractOrder;
import com.stock.engine.constant.OrderDirection;
import lombok.Data;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Comparator;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.locks.ReentrantLock;

/**
 * author Allen Wu
 * 843475854@qq.com
 * 2020/10/21
 *
 * @param <T>
 */
@Data
public class OrderEngine<T extends AbstractOrder> {

    /**
     * what exchange the engine running on
     */
    private String exchangeName;

    /**
     * buy order queue
     */
    private Map<BigDecimal, List<T>> buyOrders = new ConcurrentSkipListMap<>(
            Comparator.reverseOrder());

    /**
     * sell order queue
     */
    private Map<BigDecimal, List<T>> sellOrders = new ConcurrentSkipListMap<>();

    ReentrantLock lock = new ReentrantLock();

    @Deprecated
    public void printOrderBook() {
        System.out.println("");
        Set<BigDecimal> bid_prices = buyOrders.keySet();
        System.out.println("____ bid ____ ");
        int i=0,j=0;
        for (BigDecimal bid_price : bid_prices) {
            if (i > 10) {
                break;
            }
            System.out.println(buyOrders.get(bid_price) + " @ " + bid_price);
            i++;
        }

        Set<BigDecimal> sell_prices = sellOrders.keySet();
        System.out.println("____ask ____ ");
        for (BigDecimal sell_price : sell_prices) {
            if (j > 10) {
                break;
            }
            System.out.println(sellOrders.get(sell_price) + " @ " + sell_price);
            j++;
        }
        System.out.println("_____________ ");

    }

    /**
     * submit order;
     * @param order
     */
    public void submitOrder(T order) {
        if (OrderDirection.BUY.equals(order.getOrderDirection())) {
            if (MapUtils.isNotEmpty(sellOrders)) {
                sellOrders.keySet().forEach(sellPrice -> submitOrderToStack(order, sellPrice, sellOrders));
                if (order.getAvailableQuantity().signum() == 1) {
                    this.addToStack(order, buyOrders);
                }
            } else {
                this.addToStack(order, buyOrders);
            }
        } else {
            if (MapUtils.isNotEmpty(buyOrders)) {
                buyOrders.keySet().forEach(buyPrice -> submitOrderToStack(order, buyPrice, buyOrders));
                if (order.getAvailableQuantity().signum() == 1) {
                    this.addToStack(order, sellOrders);
                }
            } else {
                this.addToStack(order, sellOrders);
            }
        }
    }

    private void submitOrderToStack(T order, BigDecimal marketPrice, Map<BigDecimal, List<T>> stack) {
        if (order.getAvailableQuantity().signum() < 1) {
            return;
        }
        Boolean priceMatched = false;
        switch (order.getOrderDirection()) {
            case BUY:
                if (order.getPrice().compareTo(marketPrice) == 1) {
                    priceMatched = true;
                }
                break;
            case SELL:
                if (order.getPrice().compareTo(marketPrice) <= 0) {
                    priceMatched = true;
                }
                break;
        }
        if (!priceMatched) {
            return;
        }
        try {
            BigDecimal orderDealQuantity = order.getDealQuantity().get();
            lock.lock();
            if (CollectionUtils.isEmpty(stack.get(marketPrice))) {
                stack.remove(marketPrice, stack.get(marketPrice));
                return;
            }
            BigDecimal currentStackQuantity = stack.get(marketPrice).stream().map(AbstractOrder::getAvailableQuantity)
                    .reduce(BigDecimal.ZERO, BigDecimal::add);
            if (order.getAvailableQuantity().compareTo(currentStackQuantity) >= 0 &&
                    order.addDealQuantity(orderDealQuantity, currentStackQuantity)) {
                //TODO Add to executed order;
                stack.remove(marketPrice, stack.get(marketPrice));
            } else {
                order.addDealQuantity(orderDealQuantity,
                        applyQuantityForStackOrders(stack.get(marketPrice),
                                order.getAvailableQuantity()));
            }
        } finally {
            lock.unlock();
        }
    }

    private void addToStack(T order, Map<BigDecimal, List<T>> stack) {
        if (Objects.isNull(stack.computeIfPresent(order.getPrice(), (k, oldValue) -> {
            oldValue.add(order);
            return oldValue;
        }))) {
            stack.putIfAbsent(order.getPrice(), new CopyOnWriteArrayList(Arrays.asList(order)));
        }
    }

    private BigDecimal applyQuantityForStackOrders(List<T> orders, BigDecimal quantity) {
        BigDecimal appliedQuantity = BigDecimal.ZERO;
        Iterator<T> tIterator = orders.iterator();
        while (tIterator.hasNext()) {
            if (quantity.signum() == 0) {
                break;
            }
            T order = tIterator.next();
            BigDecimal dealQuantity = order.getDealQuantity().get();
            if (order.getAvailableQuantity().compareTo(quantity) == 1) {
                order.addDealQuantity(dealQuantity, quantity);
                appliedQuantity = appliedQuantity.add(quantity);
                quantity = BigDecimal.ZERO;
                //TODO Add to executed order;
            } else {
                appliedQuantity = appliedQuantity.add(order.getAvailableQuantity());
                quantity = quantity.subtract(order.getAvailableQuantity());
                order.addDealQuantity(dealQuantity, order.getAvailableQuantity());
                orders.remove(order);
                //TODO Add to executed order;
            }
        }
        return appliedQuantity;
    }

    public void reset() {
        System.out.println("size ask = " + sellOrders.size());
        System.out.println("size bid = " + buyOrders.size());
        sellOrders.clear();
        buyOrders.clear();
    }
}
