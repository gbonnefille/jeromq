/*
    Copyright (c) 2007-2014 Contributors as noted in the AUTHORS file

    This file is part of 0MQ.

    0MQ is free software; you can redistribute it and/or modify it under
    the terms of the GNU Lesser General Public License as published by
    the Free Software Foundation; either version 3 of the License, or
    (at your option) any later version.

    0MQ is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Lesser General Public License for more details.

    You should have received a copy of the GNU Lesser General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
*/

package zmq;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.junit.Test;

import static org.junit.Assert.assertThat;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;

public class TestPairTcp
{
    //  Create REQ/ROUTER wiring.

    @Test
    public void testPairTpc()
    {
        Ctx ctx = ZMQ.init(1);
        assertThat(ctx, notNullValue());
        SocketBase sb = ZMQ.socket(ctx, ZMQ.ZMQ_PAIR);
        assertThat(sb, notNullValue());
        boolean brc = ZMQ.bind(sb, "tcp://127.0.0.1:6570");
        assertThat(brc, is(true));

        SocketBase sc = ZMQ.socket(ctx, ZMQ.ZMQ_PAIR);
        assertThat(sc, notNullValue());
        brc = ZMQ.connect(sc, "tcp://127.0.0.1:6570");
        assertThat(brc, is(true));

        Helper.bounce(sb, sc);

        //  Tear down the wiring.
        ZMQ.close(sb);
        ZMQ.close(sc);
        ZMQ.term(ctx);

    }

    @Test
    public void testCongestionPairTpc() throws InterruptedException
    {
        Ctx ctx = ZMQ.init(1);
        assertThat(ctx, notNullValue());
        SocketBase sb = ZMQ.socket(ctx, ZMQ.ZMQ_PAIR);
        assertThat(sb, notNullValue());
        boolean brc = ZMQ.bind(sb, "tcp://127.0.0.1:6570");
        assertThat(brc, is(true));

        SocketBase sc = ZMQ.socket(ctx, ZMQ.ZMQ_PAIR);
        assertThat(sc, notNullValue());
        brc = ZMQ.connect(sc, "tcp://127.0.0.1:6570");
        assertThat(brc, is(true));

        sendUntilBlock(sc);

        //  Tear down the wiring.
        ZMQ.close(sb);
        ZMQ.close(sc);
        ZMQ.term(ctx);

    }

   protected void sendUntilBlock(final SocketBase sc) throws InterruptedException
   {
       final byte[] content = "12345678ABCDEFGH12345678abcdefgh".getBytes(ZMQ.CHARSET);

       runUntilBlock(new Callable<Boolean>() {
    	   public Boolean call()
    	   {
	           int rc = ZMQ.send(sc, content, 32, ZMQ.ZMQ_SNDMORE);
	           assertThat(rc, is(32));
	           rc = ZMQ.send(sc, content, 32, 0);
	           assertThat(rc, is(32));
	           return true;
    	   }
       }, 10, TimeUnit.SECONDS);
   }

   protected <V> void runUntilBlock(Callable<V> runnable, long timeout, TimeUnit unit) throws InterruptedException
   {
       boolean blocked = false;
       ExecutorService executor = Executors.newFixedThreadPool(1);
       while(!blocked)
       {
           FutureTask<V> task = new FutureTask<V>(runnable);
           executor.execute(task);
           try
           {
               task.get(timeout, unit);
               blocked = false;
           }
           catch (InterruptedException e)
           {
               throw e;
           }
           catch (ExecutionException e)
           {
               // Should not occur due to Runnable
               e.printStackTrace();
           }
           catch (TimeoutException e)
           {
               blocked = true;
           }
       }
   }
}
