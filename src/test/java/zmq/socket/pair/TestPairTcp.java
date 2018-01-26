package zmq.socket.pair;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertThat;

import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.junit.Test;

import zmq.Ctx;
import zmq.Helper;
import zmq.Msg;
import zmq.SocketBase;
import zmq.ZMQ;
import zmq.util.Utils;

public class TestPairTcp
{
    @Test
    public void testPairTcp() throws IOException
    {
        int port = Utils.findOpenPort();
        Ctx ctx = ZMQ.init(1);
        assertThat(ctx, notNullValue());
        SocketBase sb = ZMQ.socket(ctx, ZMQ.ZMQ_PAIR);
        assertThat(sb, notNullValue());
        boolean brc = ZMQ.bind(sb, "tcp://127.0.0.1:" + port);
        assertThat(brc, is(true));

        SocketBase sc = ZMQ.socket(ctx, ZMQ.ZMQ_PAIR);
        assertThat(sc, notNullValue());
        brc = ZMQ.connect(sc, "tcp://127.0.0.1:" + port);
        assertThat(brc, is(true));

        Helper.bounce(sb, sc);

        //  Tear down the wiring.
        ZMQ.close(sb);
        ZMQ.close(sc);
        ZMQ.term(ctx);
    }

    @Test
    public void testPairConnectSecondClientIssue285() throws IOException
    {
        int port = Utils.findOpenPort();
        String host = "tcp://127.0.0.1:" + port;

        Ctx ctx = ZMQ.init(1);
        assertThat(ctx, notNullValue());
        SocketBase bind = ZMQ.socket(ctx, ZMQ.ZMQ_PAIR);
        assertThat(bind, notNullValue());
        boolean brc = ZMQ.bind(bind, host);
        assertThat(brc, is(true));

        SocketBase first = ZMQ.socket(ctx, ZMQ.ZMQ_PAIR);
        assertThat(first, notNullValue());
        brc = ZMQ.connect(first, host);
        assertThat(brc, is(true));

        Helper.bounce(bind, first);

        SocketBase second = ZMQ.socket(ctx, ZMQ.ZMQ_PAIR);
        assertThat(second, notNullValue());
        brc = ZMQ.connect(second, host);
        assertThat(brc, is(true));

        int ret = ZMQ.send(bind, "data", 0);
        assertThat(ret, is(4));
        ret = ZMQ.send(bind, "datb", 0);
        assertThat(ret, is(4));
        ret = ZMQ.send(bind, "datc", 0);
        assertThat(ret, is(4));

        ZMQ.msleep(100);

        // no receiving from second connected pair
        Msg msg = ZMQ.recv(second, ZMQ.ZMQ_DONTWAIT);
        assertThat(msg, nullValue());

        // receiving from first connected pair
        msg = ZMQ.recv(first, ZMQ.ZMQ_DONTWAIT);
        assertThat(msg, notNullValue());
        assertThat(msg.data(), is("data".getBytes(ZMQ.CHARSET)));
        msg = ZMQ.recv(first, ZMQ.ZMQ_DONTWAIT);
        assertThat(msg, notNullValue());
        assertThat(msg.data(), is("datb".getBytes(ZMQ.CHARSET)));
        msg = ZMQ.recv(first, ZMQ.ZMQ_DONTWAIT);
        assertThat(msg, notNullValue());
        assertThat(msg.data(), is("datc".getBytes(ZMQ.CHARSET)));

        //  Tear down the wiring.
        ZMQ.close(bind);
        ZMQ.close(first);
        ZMQ.close(second);
        ZMQ.term(ctx);
    }

    @Test
    public void testPairMonitorIssue291() throws InterruptedException, IOException
    {
        int port = Utils.findOpenPort();
        String host = "tcp://127.0.0.1:" + port;

        Ctx ctx = ZMQ.init(1);
        assertThat(ctx, notNullValue());

        SocketBase bind = ZMQ.socket(ctx, ZMQ.ZMQ_PAIR);
        assertThat(bind, notNullValue());

        boolean rc = ZMQ.bind(bind, host);
        assertThat(rc, is(true));

        SocketBase connect = ZMQ.socket(ctx, ZMQ.ZMQ_PAIR);
        assertThat(connect, notNullValue());
        rc = ZMQ.connect(connect, host);
        assertThat(rc, is(true));

        Helper.bounce(bind, connect);

        SocketBase monitor = ZMQ.socket(ctx, ZMQ.ZMQ_PAIR);
        assertThat(monitor, notNullValue());

        rc = ZMQ.monitorSocket(connect, "inproc://events", ZMQ.ZMQ_EVENT_ALL);
        assertThat(rc, is(true));

        rc = ZMQ.bind(monitor, "inproc://events");
        assertThat(rc, is(false));

        rc = ZMQ.connect(monitor, "inproc://events");
        assertThat(rc, is(true));

        //  Tear down the wiring.
        ZMQ.close(bind);
        ZMQ.close(connect);
        ZMQ.close(monitor);
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
