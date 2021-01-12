package com.jhipster.demo.blog.repository;

import com.jhipster.demo.blog.domain.Post;
import com.jhipster.demo.blog.domain.Tag;
import com.jhipster.demo.blog.repository.rowmapper.BlogRowMapper;
import com.jhipster.demo.blog.repository.rowmapper.PostRowMapper;
import com.jhipster.demo.blog.service.EntityManager;
import com.jhipster.demo.blog.service.EntityManager.LinkTable;
import io.r2dbc.spi.Row;
import io.r2dbc.spi.RowMetadata;
import java.time.Instant;
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
 * Spring Data SQL reactive custom repository implementation for the Post entity.
 */
@SuppressWarnings("unused")
class PostRepositoryInternalImpl implements PostRepositoryInternal {

    private final DatabaseClient db;
    private final ReactiveDataAccessStrategy dataAccessStrategy;
    private final EntityManager entityManager;

    private final BlogRowMapper blogMapper;
    private final PostRowMapper postMapper;

    private static final Table entityTable = Table.aliased("post", EntityManager.ENTITY_ALIAS);
    private static final Table blogTable = Table.aliased("blog", "blog");

    private static final EntityManager.LinkTable tagLink = new LinkTable("rel_post__tag", "post_id", "tag_id");

    public PostRepositoryInternalImpl(
        DatabaseClient db,
        ReactiveDataAccessStrategy dataAccessStrategy,
        EntityManager entityManager,
        BlogRowMapper blogMapper,
        PostRowMapper postMapper
    ) {
        this.db = db;
        this.dataAccessStrategy = dataAccessStrategy;
        this.entityManager = entityManager;
        this.blogMapper = blogMapper;
        this.postMapper = postMapper;
    }

    @Override
    public Flux<Post> findAllBy(Pageable pageable) {
        return findAllBy(pageable, null);
    }

    @Override
    public Flux<Post> findAllBy(Pageable pageable, Criteria criteria) {
        return createQuery(pageable, criteria).all();
    }

    RowsFetchSpec<Post> createQuery(Pageable pageable, Criteria criteria) {
        List<Expression> columns = PostSqlHelper.getColumns(entityTable, EntityManager.ENTITY_ALIAS);
        columns.addAll(BlogSqlHelper.getColumns(blogTable, "blog"));
        SelectFromAndJoinCondition selectFrom = Select
            .builder()
            .select(columns)
            .from(entityTable)
            .leftOuterJoin(blogTable)
            .on(Column.create("blog_id", entityTable))
            .equals(Column.create("id", blogTable));

        String select = entityManager.createSelect(selectFrom, Post.class, pageable/*, criteria */);
        return db.execute(select).map(this::process);
    }

    @Override
    public Flux<Post> findAll() {
        return findAllBy(null, null);
    }

    @Override
    public Mono<Post> findById(Long id) {
        return createQuery(null, Criteria.where("id").is(id)).one();
    }

    @Override
    public Mono<Post> findOneWithEagerRelationships(Long id) {
        return findById(id);
    }

    @Override
    public Flux<Post> findAllWithEagerRelationships() {
        return findAll();
    }

    @Override
    public Flux<Post> findAllWithEagerRelationships(Pageable page) {
        return findAllBy(page);
    }

    private Post process(Row row, RowMetadata metadata) {
        Post entity = postMapper.apply(row, "e");
        entity.setBlog(blogMapper.apply(row, "blog"));
        return entity;
    }

    @Override
    public <S extends Post> Mono<S> insert(S entity) {
        return entityManager.insert(entity);
    }

    @Override
    public <S extends Post> Mono<S> save(S entity) {
        if (entity.getId() == null) {
            return insert(entity).flatMap(savedEntity -> updateRelations(savedEntity));
        } else {
            return update(entity)
                .map(
                    numberOfUpdates -> {
                        if (numberOfUpdates.intValue() <= 0) {
                            throw new IllegalStateException("Unable to update Post with id = " + entity.getId());
                        }
                        return entity;
                    }
                )
                .then(updateRelations(entity));
        }
    }

    @Override
    public Mono<Integer> update(Post entity) {
        return db.update().table(Post.class).using(entity).fetch().rowsUpdated();
    }

    @Override
    public Mono<Void> deleteById(Long entityId) {
        return deleteRelations(entityId)
            .then(db.delete().from(Post.class).matching(Criteria.from(Criteria.where("id").is(entityId))).then());
    }

    protected <S extends Post> Mono<S> updateRelations(S entity) {
        Mono<Void> result = entityManager.updateLinkTable(tagLink, entity.getId(), entity.getTags().stream().map(Tag::getId)).then();
        return result.thenReturn(entity);
    }

    protected Mono<Void> deleteRelations(Long entityId) {
        return entityManager.deleteFromLinkTable(tagLink, entityId);
    }
}

class PostSqlHelper {

    static List<Expression> getColumns(Table table, String columnPrefix) {
        List<Expression> columns = new ArrayList<>();
        columns.add(Column.aliased("id", table, columnPrefix + "_id"));
        columns.add(Column.aliased("title", table, columnPrefix + "_title"));
        columns.add(Column.aliased("content", table, columnPrefix + "_content"));
        columns.add(Column.aliased("date", table, columnPrefix + "_date"));

        columns.add(Column.aliased("blog_id", table, columnPrefix + "_blog_id"));
        return columns;
    }
}
