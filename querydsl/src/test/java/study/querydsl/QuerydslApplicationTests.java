package study.querydsl;
import com.querydsl.jpa.impl.JPAQueryFactory;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.annotation.Commit;
import org.springframework.transaction.annotation.Transactional;
import study.querydsl.entity.Hello;
import study.querydsl.entity.QHello;
import javax.persistence.EntityManager;
import java.util.List;

import static org.assertj.core.api.Assertions.*;
import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
@Transactional
@Commit
class QuerydslApplicationTests {

	@Autowired
	EntityManager em;
	@Test
	void contextLoads() {
		Hello hello = new Hello();
		em.persist(hello);	// 영속성 컨텍스트에 저장
		JPAQueryFactory query = new JPAQueryFactory(em);
//		QHello qHello = QHello.hello; //Querydsl Q타입 동작 확인
//		Hello result = query
//				.selectFrom(qHello)
//				.fetchOne();
//		Assertions.assertThat(result).isEqualTo(hello);
////lombok 동작 확인 (hello.getId())
//		Assertions.assertThat(result.getId()).isEqualTo(hello.getId());
		QHello qHello = new QHello("h");
		System.out.println("=====================================");
		Hello result = query
				.selectFrom(qHello)
				.fetchOne();
		System.out.println("=====================================");
		assertThat(result).isEqualTo(hello);
		assertThat(result.getId()).isEqualTo(hello.getId());
	}
}
