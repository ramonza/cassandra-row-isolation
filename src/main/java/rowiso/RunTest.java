package rowiso;

import com.datastax.driver.core.*;
import com.datastax.driver.core.exceptions.AlreadyExistsException;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class RunTest {

	private Cluster cluster;

	public static void main(String[] args) throws Exception {
		new RunTest().run();
	}

	public void run() throws Exception {
		cluster = Cluster.builder().addContactPoint("localhost").build();
		try {
			loadDefs();

			List<Thread> updaters = new ArrayList<>();
			for (int thread = 0; thread < 4; thread++) {
				updaters.add(startUpdater());
			}

			while (!allDone(updaters)) {
				Session readerSession = cluster.connect("row_iso");
				PreparedStatement sel = readerSession.prepare("select x, y from table1 where key = 'dummy'");
				ResultSet result = readerSession.execute(sel.bind());
				final Row row = result.one();
				final int x = row.getInt(0);
				final int y = row.getInt(1);
				if (x + y != 10) {
					System.out.printf("x = %d, y = %d - invariant fails%n", x, y);
				} else {
					System.out.println("invariant holds");
				}
				Thread.sleep(100);
			}

		} finally {
			cluster.shutdown();
		}

	}

	private boolean allDone(List<Thread> threads) {
		for (Thread t : threads) {
			if (t.isAlive())
				return false;
		}
		return true;
	}

	private Thread startUpdater() {
		final Random r = new Random();
		final Session updaterSession = cluster.connect("row_iso");
		final PreparedStatement upd = updaterSession.prepare("update table1 set x = ?, y = ? where key = 'dummy'");
		Thread t = new Thread() {
			@Override
			public void run() {
				for (int i = 0; i < 100; i++) {
					int x = r.nextInt(10);
					int y = 10 - x;
					assert x + y == 10 : "invariant";
					updaterSession.execute(upd.bind(x, y));
					try {
						Thread.sleep(10);
					} catch (InterruptedException ignored) {
					}
				}
			}
		};
		t.start();
		return t;
	}

	private void loadDefs() {
		Session session = cluster.connect();
		try {
			session.execute("create keyspace row_iso with replication = {'class': 'SimpleStrategy', 'replication_factor': 3}");
			session.execute("create table row_iso.table1(key varchar primary key, x int, y int)");
		} catch (AlreadyExistsException ignored) {}
	}

}
