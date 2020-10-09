package com.stock.engine;

import com.stock.engine.component.OrderBookEngine;
import com.stock.engine.component.RandomDouble;
import com.stock.engine.component.RandomInt;

import java.util.stream.IntStream;

/**
 * Hello world!
 *
 */
public class App 
{
    static RandomDouble randomDouble = new RandomDouble();
    static RandomInt randomInt = new RandomInt();

    static {
        randomDouble.initialize();
        randomInt.initialize( );
    }


    public static void main( String[] args )
    {
        OrderBookEngine engine = new OrderBookEngine();
        long n1 = System.currentTimeMillis();
        int ITERATIONS = 10000000;
        IntStream.range(0, ITERATIONS).parallel().forEach(i -> {
            if (i % 100000 == 0) {
                System.out.println(i + " orders sent");
            }
            if (randomDouble.nextDouble() > 50) {
                double price = randomDouble.nextDouble();
                int qty = randomInt.nextInt();
                engine.receiveOrder(price, qty, true);
            } else {
                double price = randomDouble.nextDouble();
                int qty = randomInt.nextInt();
                engine.receiveOrder(price, qty, false);
            }
        });

        long elapsedTimeMillis = System.currentTimeMillis() - n1;
        System.out.println("Total execute time:" + elapsedTimeMillis/1000);
        long elapsedTimeMicros = elapsedTimeMillis * 1000;

        System.out.println(((double)elapsedTimeMicros) / ITERATIONS + " us on average");
        engine.printOrderBook();
        engine.reset();
    }
}
