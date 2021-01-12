package com.jhipster.demo.blog.repository;

import com.jhipster.demo.blog.domain.Tag;
import com.jhipster.demo.blog.repository.rowmapper.TagRowMapper;
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
import org.springframework.data.relational.core.sql.SelectBuilder.SelectFromAndJoin;
import org.springframework.data.relational.core.sql.Table;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * Spring Data SQL reactive custom repository implementation for the Tag entity.
 */
@SuppressWarnings("unused")
class TagRepositoryInternalImpl implements TagRepositoryInternal {

    private final DatabaseClient db;
    private final ReactiveDataAccessStrategy dataAccessStrategy;
    private final EntityManager entityManager;

    private final TagRowMapper tagMapper;

    private static final Table entityTable = Table.aliased("tag", EntityManager.ENTITY_ALIAS);

    public TagRepositoryInternalImpl(
        DatabaseClient db,
        ReactiveDataAccessStrategy dataAccessStrategy,
        EntityManager entityManager,
        TagRowMapper tagMapper
    ) {
        this.db = db;
        this.dataAccessStrategy = dataAccessStrategy;
        this.entityManager = entityManager;
        this.tagMapper = tagMapper;
    }

    @Override
    public Flux<Tag> findAllBy(Pageable pageable) {
        return findAllBy(pageable, null);
    }

    @Override
    public Flux<Tag> findAllBy(Pageable pageable, Criteria criteria) {
        return createQuery(pageable, criteria).all();
    }

    RowsFetchSpec<Tag> createQuery(Pageable pageable, Criteria criteria) {
        List<Expression> columns = TagSqlHelper.getColumns(entityTable, EntityManager.ENTITY_ALIAS);
        SelectFromAndJoin selectFrom = Select.builder().select(columns).from(entityTable);

        String select = entityManager.createSelect(selectFrom, Tag.class, pageable/*, criteria */);
        return db.execute(select).map(this::process);
    }

    @Override
    public Flux<Tag> findAll() {
        return findAllBy(null, null);
    }

    @Override
    public Mono<Tag> findById(Long id) {
        return createQuery(null, Criteria.where("id").is(id)).one();
    }

    private Tag process(Row row, RowMetadata metadata) {
        Tag entity = tagMapper.apply(row, "e");
        return entity;
    }

    @Override
    public <S extends Tag> Mono<S> insert(S entity) {
        return entityManager.insert(entity);
    }

    @Override
    public <S extends Tag> Mono<S> save(S entity) {
        if (entity.getId() == null) {
            return insert(entity);
        } else {
            return update(entity)
                .map(
                    numberOfUpdates -> {
                        if (numberOfUpdates.intValue() <= 0) {
                            throw new IllegalStateException("Unable to update Tag with id = " + entity.getId());
                        }
                        return entity;
                    }
                );
        }
    }

    @Override
    public Mono<Integer> update(Tag entity) {
        return db.update().table(Tag.class).using(entity).fetch().rowsUpdated();
    }
}

class TagSqlHelper {

    static List<Expression> getColumns(Table table, String columnPrefix) {
        List<Expression> columns = new ArrayList<>();
        columns.add(Column.aliased("id", table, columnPrefix + "_id"));
        columns.add(Column.aliased("name", table, columnPrefix + "_name"));

        return columns;
    }
}
