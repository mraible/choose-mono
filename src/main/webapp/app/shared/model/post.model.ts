import { IBlog } from '@/shared/model/blog.model';
import { ITag } from '@/shared/model/tag.model';

export interface IPost {
  id?: string;
  title?: string;
  content?: string;
  date?: Date;
  blog?: IBlog;
  tags?: ITag[];
}

export class Post implements IPost {
  constructor(
    public id?: string,
    public title?: string,
    public content?: string,
    public date?: Date,
    public blog?: IBlog,
    public tags?: ITag[]
  ) {}
}
