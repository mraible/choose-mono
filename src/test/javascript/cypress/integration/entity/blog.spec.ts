import {
  entityTableSelector,
  entityDetailsButtonSelector,
  entityDetailsBackButtonSelector,
  entityCreateButtonSelector,
  entityCreateSaveButtonSelector,
  entityEditButtonSelector,
  entityDeleteButtonSelector,
  entityConfirmDeleteButtonSelector,
} from '../../support/entity';

describe('Blog e2e test', () => {
  let startingEntitiesCount = 0;

  beforeEach(() => {
    cy.getOauth2Data();
    cy.get('@oauth2Data').then(oauth2Data => {
      cy.keycloackLogin(oauth2Data, 'user');
    });
    cy.server();
    cy.route('GET', '/api/blogs*').as('entitiesRequest');
    cy.visit('');
    cy.clickOnEntityMenuItem('blog');
    cy.wait('@entitiesRequest')
      .its('responseBody')
      .then(array => {
        startingEntitiesCount = array.length;
      });
    cy.visit('/');
  });

  afterEach(() => {
    cy.get('@oauth2Data').then(oauth2Data => {
      cy.keycloackLogout(oauth2Data);
    });
    cy.clearCache();
  });

  it('should load Blogs', () => {
    cy.server();
    cy.route('GET', '/api/blogs*').as('entitiesRequest');
    cy.visit('/');
    cy.clickOnEntityMenuItem('blog');
    cy.wait('@entitiesRequest');
    cy.getEntityHeading('Blog').should('exist');
    if (startingEntitiesCount === 0) {
      cy.get(entityTableSelector).should('not.exist');
    } else {
      cy.get(entityTableSelector).should('have.lengthOf', startingEntitiesCount);
    }
    cy.visit('/');
  });

  it('should load details Blog page', () => {
    cy.server();
    cy.route('GET', '/api/blogs*').as('entitiesRequest');
    cy.visit('/');
    cy.clickOnEntityMenuItem('blog');
    cy.wait('@entitiesRequest');
    if (startingEntitiesCount > 0) {
      cy.get(entityDetailsButtonSelector).first().click({ force: true });
      cy.getEntityDetailsHeading('blog');
      cy.get(entityDetailsBackButtonSelector).should('exist');
    }
    cy.visit('/');
  });

  it('should load create Blog page', () => {
    cy.server();
    cy.route('GET', '/api/blogs*').as('entitiesRequest');
    cy.visit('/');
    cy.clickOnEntityMenuItem('blog');
    cy.wait('@entitiesRequest');
    cy.get(entityCreateButtonSelector).click({ force: true });
    cy.getEntityCreateUpdateHeading('Blog');
    cy.get(entityCreateSaveButtonSelector).should('exist');
    cy.visit('/');
  });

  it('should load edit Blog page', () => {
    cy.server();
    cy.route('GET', '/api/blogs*').as('entitiesRequest');
    cy.visit('/');
    cy.clickOnEntityMenuItem('blog');
    cy.wait('@entitiesRequest');
    if (startingEntitiesCount > 0) {
      cy.get(entityEditButtonSelector).first().click({ force: true });
      cy.getEntityCreateUpdateHeading('Blog');
      cy.get(entityCreateSaveButtonSelector).should('exist');
    }
    cy.visit('/');
  });

  it('should create an instance of Blog', () => {
    cy.server();
    cy.route('GET', '/api/blogs*').as('entitiesRequest');
    cy.visit('/');
    cy.clickOnEntityMenuItem('blog');
    cy.wait('@entitiesRequest');
    cy.get(entityCreateButtonSelector).click({ force: true });
    cy.getEntityCreateUpdateHeading('Blog');

    cy.get(`[data-cy="name"]`)
      .type('architectures Assistant Grocery', { force: true })
      .invoke('val')
      .should('match', new RegExp('architectures Assistant Grocery'));

    cy.get(`[data-cy="handle"]`)
      .type('Forward value-added', { force: true })
      .invoke('val')
      .should('match', new RegExp('Forward value-added'));

    cy.setFieldSelectToLastOfEntity('user');

    cy.get(entityCreateSaveButtonSelector).click({ force: true });
    cy.scrollTo('top', { ensureScrollable: false });
    cy.get(entityCreateSaveButtonSelector).should('not.exist');
    cy.route('GET', '/api/blogs*').as('entitiesRequestAfterCreate');
    cy.visit('/');
    cy.clickOnEntityMenuItem('blog');
    cy.wait('@entitiesRequestAfterCreate');
    cy.get(entityTableSelector).should('have.lengthOf', startingEntitiesCount + 1);
    cy.visit('/');
  });

  it('should delete last instance of Blog', () => {
    cy.server();
    cy.route('GET', '/api/blogs*').as('entitiesRequest');
    cy.route('DELETE', '/api/blogs/*').as('deleteEntityRequest');
    cy.visit('/');
    cy.clickOnEntityMenuItem('blog');
    cy.wait('@entitiesRequest')
      .its('responseBody')
      .then(array => {
        startingEntitiesCount = array.length;
        if (startingEntitiesCount > 0) {
          cy.get(entityTableSelector).should('have.lengthOf', startingEntitiesCount);
          cy.get(entityDeleteButtonSelector).last().click({ force: true });
          cy.getEntityDeleteDialogHeading('blog').should('exist');
          cy.get(entityConfirmDeleteButtonSelector).click({ force: true });
          cy.wait('@deleteEntityRequest');
          cy.route('GET', '/api/blogs*').as('entitiesRequestAfterDelete');
          cy.visit('/');
          cy.clickOnEntityMenuItem('blog');
          cy.wait('@entitiesRequestAfterDelete');
          cy.get(entityTableSelector).should('have.lengthOf', startingEntitiesCount - 1);
        }
        cy.visit('/');
      });
  });
});
