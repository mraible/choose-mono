import { Component, Vue, Inject } from 'vue-property-decorator';

import { required, minLength } from 'vuelidate/lib/validators';

import { IBlog, Blog } from '@/shared/model/blog.model';
import BlogService from './blog.service';

const validations: any = {
  blog: {
    name: {
      required,
      minLength: minLength(3),
    },
    handle: {
      required,
      minLength: minLength(2),
    },
  },
};

@Component({
  validations,
})
export default class BlogUpdate extends Vue {
  @Inject('blogService') private blogService: () => BlogService;
  public blog: IBlog = new Blog();
  public isSaving = false;
  public currentLanguage = '';

  beforeRouteEnter(to, from, next) {
    next(vm => {
      if (to.params.blogId) {
        vm.retrieveBlog(to.params.blogId);
      }
      vm.initRelationships();
    });
  }

  created(): void {
    this.currentLanguage = this.$store.getters.currentLanguage;
    this.$store.watch(
      () => this.$store.getters.currentLanguage,
      () => {
        this.currentLanguage = this.$store.getters.currentLanguage;
      }
    );
  }

  public save(): void {
    this.isSaving = true;
    if (this.blog.id) {
      this.blogService()
        .update(this.blog)
        .then(param => {
          this.isSaving = false;
          this.$router.go(-1);
          const message = this.$t('blogApp.blog.updated', { param: param.id });
          return this.$root.$bvToast.toast(message.toString(), {
            toaster: 'b-toaster-top-center',
            title: 'Info',
            variant: 'info',
            solid: true,
            autoHideDelay: 5000,
          });
        });
    } else {
      this.blogService()
        .create(this.blog)
        .then(param => {
          this.isSaving = false;
          this.$router.go(-1);
          const message = this.$t('blogApp.blog.created', { param: param.id });
          this.$root.$bvToast.toast(message.toString(), {
            toaster: 'b-toaster-top-center',
            title: 'Success',
            variant: 'success',
            solid: true,
            autoHideDelay: 5000,
          });
        });
    }
  }

  public retrieveBlog(blogId): void {
    this.blogService()
      .find(blogId)
      .then(res => {
        this.blog = res;
      });
  }

  public previousState(): void {
    this.$router.go(-1);
  }

  public initRelationships(): void {}
}
