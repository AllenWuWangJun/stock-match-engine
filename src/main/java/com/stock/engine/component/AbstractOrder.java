package com.stock.engine.component;

import com.stock.engine.constant.OrderDirection;
import lombok.Getter;
import lombok.Setter;

import java.math.BigDecimal;
import java.util.concurrent.atomic.AtomicReference;


public abstract class AbstractOrder {
    /**
     * price
     */
    @Getter
    @Setter()
    protected BigDecimal price;
    /**
     * submit quantity
     */
    @Getter
    protected BigDecimal submitQuantity;
    /**
     * deal quantity
     */
    @Getter
    protected AtomicReference<BigDecimal> dealQuantity = new AtomicReference<>(BigDecimal.ZERO);

    /**
     * available quantity
     */
    @Getter
    protected BigDecimal availableQuantity;
    /**
     * order direction
     */
    @Getter
    @Setter()
    protected OrderDirection orderDirection;

    public boolean addDealQuantity(BigDecimal originalQuantity, BigDecimal targetQuantity) {
        if (this.dealQuantity.compareAndSet(originalQuantity, originalQuantity.add(targetQuantity))) {
            availableQuantity = submitQuantity.subtract(dealQuantity.get());
            return true;
        }
        return false;
    }

    public AbstractOrder(BigDecimal price, BigDecimal submitQuantity, OrderDirection orderDirection) {
        this.price = price;
        this.submitQuantity = submitQuantity;
        this.orderDirection = orderDirection;
        this.availableQuantity = submitQuantity;
    }
}
