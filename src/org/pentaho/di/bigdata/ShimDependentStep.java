package org.pentaho.di.bigdata;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Documented
@Retention( RetentionPolicy.RUNTIME )
@Target( ElementType.TYPE )
public @interface ShimDependentStep {
  /**
   * @return The ID of the step. You can specify more than one ID in a comma separated format: id1,id2,id3 for
   *         deprecation purposes.
   */
  String id();

  String name();

  String description() default "";

  String image();

  /**
   * @return True if a separate class loader is needed every time this class is instantiated
   */
  boolean isSeparateClassLoaderNeeded() default false;

  String classLoaderGroup() default "";

  String categoryDescription() default "";

  String i18nPackageName() default "";

  String documentationUrl() default "";

  String casesUrl() default "";

  String forumUrl() default "";
}
