import { IBlog } from '@/shared/model/blog.model';
import { ITag } from '@/shared/model/tag.model';

export interface IPost {
  id?: number;
  title?: string;
  content?: string;
  date?: Date;
  blog?: IBlog;
  tags?: ITag[];
}

export class Post implements IPost {
  constructor(
    public id?: number,
    public title?: string,
    public content?: string,
    public date?: Date,
    public blog?: IBlog,
    public tags?: ITag[]
  ) {}
}
