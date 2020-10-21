package com.stock.engine.component;

import com.stock.engine.constant.OrderDirection;
import lombok.Data;


import java.io.Serializable;
import java.math.BigDecimal;


@Data
public class Order extends AbstractOrder implements Serializable {
    private static final long serialVersionUID = 3288167018644601051L;

    public Order(BigDecimal price, BigDecimal submitQuantity, OrderDirection orderDirection) {
        super(price, submitQuantity, orderDirection);
    }

    @Override
    public String toString() {
        return "Order price:" + this.price
                + ", submit quantity:" + this.submitQuantity + ", deal quantity:" + this.dealQuantity.get()
                + ", availableQuantity:" + this.availableQuantity + ", order direction:" + this.orderDirection;
    }
}
