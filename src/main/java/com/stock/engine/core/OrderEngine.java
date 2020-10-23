package com.stock.engine.core;

import com.stock.engine.component.AbstractOrder;
import com.stock.engine.constant.OrderDirection;
import lombok.Data;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.collections4.map.LinkedMap;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Comparator;

import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentSkipListMap;

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
    private Map<BigDecimal, Queue<T>> buyOrders = new ConcurrentSkipListMap<>(
            Comparator.reverseOrder());

    /**
     * sell order queue
     */
    private Map<BigDecimal, Queue<T>> sellOrders = new ConcurrentSkipListMap<>(
            Comparator.naturalOrder());

    public Map<BigDecimal, BigDecimal> getTopOrders(int level, OrderDirection orderDirection) {
        Map<BigDecimal, BigDecimal> result = new LinkedMap<>();
        Set<BigDecimal> prices = orderDirection.equals(OrderDirection.BUY) ? buyOrders.keySet() : sellOrders.keySet();
        int i = 0;
        for (BigDecimal price : prices) {
            if (i > level) {
                break;
            }
            result.put(price,
                    orderDirection.equals(OrderDirection.BUY) ? buyOrders.get(price).stream()
                            .map(AbstractOrder::getAvailableQuantity).reduce(BigDecimal.ZERO, BigDecimal::add) : sellOrders.get(price).stream()
                            .map(AbstractOrder::getAvailableQuantity).reduce(BigDecimal.ZERO, BigDecimal::add));
            i++;
        }
        return result;
    }

    public void printOrderBook() {
        System.out.println("");
        System.out.println("____ bid ____ ");
        System.out.println(getTopOrders(10, OrderDirection.BUY));
        System.out.println("____ask ____ ");
        System.out.println(getTopOrders(10, OrderDirection.SELL));
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

    private void submitOrderToStack(T order, BigDecimal marketPrice, Map<BigDecimal, Queue<T>> stack) {
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
        BigDecimal orderDealQuantity = order.getDealQuantity().get();
        Optional<Queue<T>> priceQueue = Optional.ofNullable(stack.get(marketPrice));
        if (priceQueue.isEmpty()) {
            stack.remove(marketPrice, null);
            return;
        }
        synchronized (priceQueue) {
            if (CollectionUtils.isEmpty(priceQueue.get())) {
                stack.remove(marketPrice, priceQueue.get());
                return;
            }
            BigDecimal currentStackQuantity = priceQueue.get().stream().map(AbstractOrder::getAvailableQuantity)
                    .reduce(BigDecimal.ZERO, BigDecimal::add);
            if (order.getAvailableQuantity().compareTo(currentStackQuantity) >= 0 &&
                    order.addDealQuantity(orderDealQuantity, currentStackQuantity)) {
                //TODO Add to executed order;
                stack.remove(marketPrice, priceQueue.get());
            } else {
                order.addDealQuantity(orderDealQuantity,
                        applyQuantityForStackOrders(priceQueue.get(),
                                order.getAvailableQuantity()));
            }
        }
    }

    private void addToStack(T order, Map<BigDecimal, Queue<T>> stack) {
        if (Objects.isNull(stack.computeIfPresent(order.getPrice(), (k, oldValue) -> {
            oldValue.add(order);
            return oldValue;
        }))) {
            stack.putIfAbsent(order.getPrice(), new ConcurrentLinkedQueue<>(Arrays.asList(order)));
        }
    }

    private BigDecimal applyQuantityForStackOrders(Queue<T> orders, BigDecimal quantity) {
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
