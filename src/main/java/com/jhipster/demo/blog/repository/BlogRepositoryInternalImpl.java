package com.jhipster.demo.blog.repository;

import com.jhipster.demo.blog.domain.Blog;
import com.jhipster.demo.blog.repository.rowmapper.BlogRowMapper;
import com.jhipster.demo.blog.repository.rowmapper.UserRowMapper;
import com.jhipster.demo.blog.service.EntityManager;
import io.r2dbc.spi.Row;
import io.r2dbc.spi.RowMetadata;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.BiFunction;
import org.springframework.data.domain.Pageable;
import org.springframework.data.r2dbc.core.DatabaseClient;
import org.springframework.data.r2dbc.core.DatabaseClient.GenericInsertSpec;
import org.springframework.data.r2dbc.core.ReactiveDataAccessStrategy;
import org.springframework.data.r2dbc.core.RowsFetchSpec;
import org.springframework.data.r2dbc.mapping.OutboundRow;
import org.springframework.data.r2dbc.mapping.SettableValue;
import org.springframework.data.relational.core.query.Criteria;
import org.springframework.data.relational.core.sql.Column;
import org.springframework.data.relational.core.sql.Expression;
import org.springframework.data.relational.core.sql.Select;
import org.springframework.data.relational.core.sql.SelectBuilder.SelectFromAndJoinCondition;
import org.springframework.data.relational.core.sql.Table;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * Spring Data SQL reactive custom repository implementation for the Blog entity.
 */
@SuppressWarnings("unused")
class BlogRepositoryInternalImpl implements BlogRepositoryInternal {

    private final DatabaseClient db;
    private final ReactiveDataAccessStrategy dataAccessStrategy;
    private final EntityManager entityManager;

    private final UserRowMapper userMapper;
    private final BlogRowMapper blogMapper;

    private static final Table entityTable = Table.aliased("blog", EntityManager.ENTITY_ALIAS);
    private static final Table userTable = Table.aliased("jhi_user", "e_user");

    public BlogRepositoryInternalImpl(
        DatabaseClient db,
        ReactiveDataAccessStrategy dataAccessStrategy,
        EntityManager entityManager,
        UserRowMapper userMapper,
        BlogRowMapper blogMapper
    ) {
        this.db = db;
        this.dataAccessStrategy = dataAccessStrategy;
        this.entityManager = entityManager;
        this.userMapper = userMapper;
        this.blogMapper = blogMapper;
    }

    @Override
    public Flux<Blog> findAllBy(Pageable pageable) {
        return findAllBy(pageable, null);
    }

    @Override
    public Flux<Blog> findAllBy(Pageable pageable, Criteria criteria) {
        return createQuery(pageable, criteria).all();
    }

    RowsFetchSpec<Blog> createQuery(Pageable pageable, Criteria criteria) {
        List<Expression> columns = BlogSqlHelper.getColumns(entityTable, EntityManager.ENTITY_ALIAS);
        columns.addAll(UserSqlHelper.getColumns(userTable, "user"));
        SelectFromAndJoinCondition selectFrom = Select
            .builder()
            .select(columns)
            .from(entityTable)
            .leftOuterJoin(userTable)
            .on(Column.create("user_id", entityTable))
            .equals(Column.create("id", userTable));

        String select = entityManager.createSelect(selectFrom, Blog.class, pageable/*, criteria */);
        return db.execute(select).map(this::process);
    }

    @Override
    public Flux<Blog> findAll() {
        return findAllBy(null, null);
    }

    @Override
    public Mono<Blog> findById(Long id) {
        return createQuery(null, Criteria.where("id").is(id)).one();
    }

    private Blog process(Row row, RowMetadata metadata) {
        Blog entity = blogMapper.apply(row, "e");
        entity.setUser(userMapper.apply(row, "user"));
        return entity;
    }

    @Override
    public <S extends Blog> Mono<S> insert(S entity) {
        return entityManager.insert(entity);
    }

    @Override
    public <S extends Blog> Mono<S> save(S entity) {
        if (entity.getId() == null) {
            return insert(entity);
        } else {
            return update(entity)
                .map(
                    numberOfUpdates -> {
                        if (numberOfUpdates.intValue() <= 0) {
                            throw new IllegalStateException("Unable to update Blog with id = " + entity.getId());
                        }
                        return entity;
                    }
                );
        }
    }

    @Override
    public Mono<Integer> update(Blog entity) {
        return db.update().table(Blog.class).using(entity).fetch().rowsUpdated();
    }
}

class BlogSqlHelper {

    static List<Expression> getColumns(Table table, String columnPrefix) {
        List<Expression> columns = new ArrayList<>();
        columns.add(Column.aliased("id", table, columnPrefix + "_id"));
        columns.add(Column.aliased("name", table, columnPrefix + "_name"));
        columns.add(Column.aliased("handle", table, columnPrefix + "_handle"));

        columns.add(Column.aliased("user_id", table, columnPrefix + "_user_id"));
        return columns;
    }
}
