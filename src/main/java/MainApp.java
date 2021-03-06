import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.cfg.Configuration;

import javax.persistence.LockModeType;
import javax.persistence.OptimisticLockException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;

public class MainApp {
    public static void main(String[] args) throws IOException {
        int rawCount = 40;
        int threadCount = 8;

        dropAndCreateTable();
        fillTable(rawCount, 0);
        updateTableWithOptimisticLock(rawCount, threadCount);
        updateTableWithPessimisticLock(rawCount, threadCount);
        getSumOfVal();
    }


    public static void getSumOfVal() {
        SessionFactory factory = new Configuration()
                .configure("hibernate.cfg.xml")
                .addAnnotatedClass(Item.class)
                .buildSessionFactory();

        Session session = null;

        try {
            session = factory.getCurrentSession();
            session.beginTransaction();

            Object o = session.createNativeQuery("SELECT sum(val) FROM items;").getSingleResult();

            System.out.println("Sum : " + o);

            session.getTransaction().commit();
        } finally {
            factory.close();
            session.close();
        }
    }

    public static void updateTableWithPessimisticLock(int rawCount, int threadCount) {
        SessionFactory factory = new Configuration()
                .configure("hibernate.cfg.xml")
                .addAnnotatedClass(Item.class)
                .buildSessionFactory();

        CountDownLatch cdl = new CountDownLatch(threadCount);

        long time = System.currentTimeMillis();

        for (int t=0;t<threadCount;t++) {
            new Thread(() -> {
                System.out.println("Thread #" + Thread.currentThread().getName() + " started");

                for (int i=0;i<20000;i++) {
                    int idx = (int) (Math.random() * rawCount);

                    boolean updated = false;
                    while (!updated) {
                        Session session = factory.getCurrentSession();
                        session.beginTransaction();

                        try {
                            Thread.sleep(5);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }

                        try {
                            String request = "FROM Items WHERE id = " + (idx + 1);
                            Item item = session
                                    .createQuery("FROM Item i WHERE i.id = :id", Item.class)
                                    .setLockMode(LockModeType.PESSIMISTIC_WRITE)
                                    .setParameter("id", idx+1)
                                    .getSingleResult();

                            item.setVal(item.getVal() + 1);

//                        System.out.println(Thread.currentThread().getName() + " : " + item);


                            session.getTransaction().commit();
                            updated = true;
                        } catch (OptimisticLockException e) {
                            session.getTransaction().rollback();
//                            System.out.println("Thread #" + Thread.currentThread().getName() + " rollback");
//                        e.printStackTrace();
                        }

                        if (session != null) {
                            session.close();
                        }
                    }
                }

                cdl.countDown();
            }
            ).start();
        }

        try {
            System.out.println("cdl.await() : ");
            cdl.await();
            System.out.println("OK");
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        System.out.println("PessimisticLock Time : " + (System.currentTimeMillis() - time));
    }

    public static void updateTableWithOptimisticLock(int rawCount, int threadCount) {
        SessionFactory factory = new Configuration()
                .configure("hibernate.cfg.xml")
                .addAnnotatedClass(Item.class)
                .buildSessionFactory();

        CountDownLatch cdl = new CountDownLatch(threadCount);

        long time = System.currentTimeMillis();

        for (int t=0;t<threadCount;t++) {
            new Thread(() -> {
                System.out.println("Thread #" + Thread.currentThread().getName() + " started");

                for (int i=0;i<20000;i++) {
                    int idx = (int) (Math.random() * rawCount);

                    boolean updated = false;
                    while (!updated) {
                        Session session = factory.getCurrentSession();
                        session.beginTransaction();

                        try {
                            Thread.sleep(5);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }

                        try {
                            Item item = session.get(Item.class, idx + 1);
                            item.setVal(item.getVal() + 1);

//                        System.out.println(Thread.currentThread().getName() + " : " + item);


                            session.getTransaction().commit();
                            updated = true;
                        } catch (OptimisticLockException e) {
                            session.getTransaction().rollback();
//                            System.out.println("Thread #" + Thread.currentThread().getName() + " rollback");
//                        e.printStackTrace();
                        }

                        if (session != null) {
                            session.close();
                        }
                    }
                }

                cdl.countDown();
            }
            ).start();
        }

        try {
            System.out.println("cdl.await() : ");
            cdl.await();
            System.out.println("OK");
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        System.out.println("OptimisticLock Time : " + (System.currentTimeMillis() - time));
    }

    public static void fillTable(int rawCount, int value) {
        SessionFactory factory = new Configuration()
                .configure("hibernate.cfg.xml")
                .addAnnotatedClass(Item.class)
                .buildSessionFactory();

        Session session = null;

        try {
            session = factory.getCurrentSession();
            session.beginTransaction();

            for (int i=0;i<rawCount;i++) session.save(new Item(value));

            session.getTransaction().commit();
        } finally {
            factory.close();
            session.close();
        }
    }



    public static void dropAndCreateTable() throws IOException {
        SessionFactory factory = new Configuration()
                .configure("hibernate.cfg.xml")
                .buildSessionFactory();

        String sql = Files.lines(Paths.get("drop-and-create.sql")).collect(Collectors.joining(" "));

        Session session = null;

        try {
            session = factory.getCurrentSession();
            session.beginTransaction();
            session.createNativeQuery(sql).executeUpdate();
            session.getTransaction().commit();
        } finally {
            factory.close();
            session.close();
        }

    }
}
