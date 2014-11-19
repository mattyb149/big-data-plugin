package org.pentaho.di.bigdata;

import org.apache.commons.vfs.FileObject;
import org.apache.commons.vfs.FileSelectInfo;
import org.apache.commons.vfs.FileSelector;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.annotations.Step;
import org.pentaho.di.core.exception.KettleFileException;
import org.pentaho.di.core.exception.KettlePluginException;
import org.pentaho.di.core.hadoop.HadoopConfigurationBootstrap;
import org.pentaho.di.core.plugins.KettleURLClassLoader;
import org.pentaho.di.core.plugins.PluginAnnotationType;
import org.pentaho.di.core.plugins.PluginFolder;
import org.pentaho.di.core.plugins.PluginFolderInterface;
import org.pentaho.di.core.plugins.PluginInterface;
import org.pentaho.di.core.plugins.PluginMainClassType;
import org.pentaho.di.core.plugins.PluginRegistry;
import org.pentaho.di.core.plugins.PluginTypeCategoriesOrder;
import org.pentaho.di.core.plugins.StepPluginType;
import org.pentaho.di.core.vfs.KettleVFS;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMetaInterface;
import org.pentaho.hadoop.shim.ConfigurationException;

import java.io.File;
import java.lang.annotation.Annotation;
import java.net.URL;
import java.net.URLDecoder;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

@PluginTypeCategoriesOrder(
  getNaturalCategoriesOrder = {
    "BaseStep.Category.Input", "BaseStep.Category.Output", "BaseStep.Category.Transform",
    "BaseStep.Category.Utility", "BaseStep.Category.Flow", "BaseStep.Category.Scripting",
    "BaseStep.Category.Lookup", "BaseStep.Category.Joins", "BaseStep.Category.DataWarehouse",
    "BaseStep.Category.Validation", "BaseStep.Category.Statistics", "BaseStep.Category.DataMining",
    "BaseStep.Category.BigData", "BaseStep.Category.Agile", "BaseStep.Category.DataQuality",
    "BaseStep.Category.Cryptography", "BaseStep.Category.Palo", "BaseStep.Category.OpenERP",
    "BaseStep.Category.Job", "BaseStep.Category.Mapping", "BaseStep.Category.Bulk",
    "BaseStep.Category.Inline", "BaseStep.Category.Experimental", "BaseStep.Category.Deprecated" },
  i18nPackageClass = StepInterface.class )

@PluginMainClassType( StepMetaInterface.class )
@PluginAnnotationType( Step.class )
public class ShimDependentStepPluginType extends StepPluginType {
  private static final ShimDependentStepPluginType instance = new ShimDependentStepPluginType();
  private final Map<Set<String>, KettleURLClassLoader> classLoaderMap =
    new HashMap<Set<String>, KettleURLClassLoader>();

  private ShimDependentStepPluginType() {
    super( ShimDependentStep.class, "SHIM_DEPENDENT_STEP", "Shim Dependent Step" );
  }

  public static ShimDependentStepPluginType getInstance() {
    return instance;
  }

  @Override
  public List<PluginFolderInterface> getPluginFolders() {
    return Arrays.<PluginFolderInterface>asList( new PluginFolder( new File( ShimDependentStepPluginType.class
      .getProtectionDomain().getCodeSource().getLocation().getPath() ).getParentFile().toURI().toString()
      + "plugins/", false, true ) {
      @Override
      public FileObject[] findJarFiles( final boolean includeLibJars ) throws KettleFileException {
        try {
          // Find all the jar files in this folder...
          //
          FileObject folderObject = KettleVFS.getFileObject( this.getFolder() );
          FileObject[] fileObjects = folderObject.findFiles( new FileSelector() {
            @Override
            public boolean traverseDescendents( FileSelectInfo fileSelectInfo ) throws Exception {
              FileObject fileObject = fileSelectInfo.getFile();
              String folder = fileObject.getName().getBaseName();
              return includeLibJars || !"lib".equals( folder );
            }

            @Override
            public boolean includeFile( FileSelectInfo fileSelectInfo ) throws Exception {
              return fileSelectInfo.getFile().toString().endsWith( ".jar" );
            }
          } );

          return fileObjects;
        } catch ( Exception e ) {
          throw new KettleFileException( "Unable to list jar files in plugin folder '" + toString() + "'", e );
        }
      }
    } );
  }

  @Override
  public void handlePluginAnnotation( Class<?> clazz, Annotation annotation, List<String> libraries,
                                      boolean nativePluginType, URL pluginFolder ) throws KettlePluginException {
    String idList = extractID( annotation );
    if ( Const.isEmpty( idList ) ) {
      throw new KettlePluginException( "No ID specified for plugin with class: " + clazz.getName() );
    }

    // Only one ID for now
    String[] ids = idList.split( "," );
    super.handlePluginAnnotation( clazz, annotation, libraries, nativePluginType, pluginFolder );
    PluginInterface plugin =
      PluginRegistry.getInstance().findPluginWithId( ShimDependentStepPluginType.class, ids[0] );
    URL[] urls = new URL[libraries.size()];
    for ( int i = 0; i < libraries.size(); i++ ) {
      File jarfile = new File( libraries.get( i ) );
      try {
        urls[i] = new URL( URLDecoder.decode( jarfile.toURI().toURL().toString(), "UTF-8" ) );
      } catch ( Exception e ) {
        throw new KettlePluginException( e );
      }
    }
    try {
      Set<String> librarySet = new HashSet<String>( libraries );
      KettleURLClassLoader classloader = classLoaderMap.get( librarySet );
      if ( classloader == null ) {
        classloader =
          new KettleURLClassLoader( urls, HadoopConfigurationBootstrap.getHadoopConfigurationProvider()
            .getActiveConfiguration().getHadoopShim().getClass().getClassLoader() );
        classLoaderMap.put( librarySet, classloader );
      }
      PluginRegistry.getInstance().addClassLoader( classloader, plugin );
    } catch ( ConfigurationException e ) {
      throw new KettlePluginException( e );
    }
  }

  @Override
  protected void registerNatives() throws KettlePluginException {
    // noop
  }

  @Override
  protected String extractCategory( Annotation annotation ) {
    return ( (ShimDependentStep) annotation ).categoryDescription();
  }

  @Override
  protected String extractDesc( Annotation annotation ) {
    return ( (ShimDependentStep) annotation ).description();
  }

  @Override
  protected String extractID( Annotation annotation ) {
    return ( (ShimDependentStep) annotation ).id();
  }

  @Override
  protected String extractName( Annotation annotation ) {
    return ( (ShimDependentStep) annotation ).name();
  }

  @Override
  protected String extractImageFile( Annotation annotation ) {
    return ( (ShimDependentStep) annotation ).image();
  }

  @Override
  protected String extractI18nPackageName( Annotation annotation ) {
    return ( (ShimDependentStep) annotation ).i18nPackageName();
  }

  @Override
  protected String extractDocumentationUrl( Annotation annotation ) {
    return ( (ShimDependentStep) annotation ).documentationUrl();
  }

  @Override
  protected String extractCasesUrl( Annotation annotation ) {
    return ( (ShimDependentStep) annotation ).casesUrl();
  }

  @Override
  protected boolean extractSeparateClassLoader( Annotation annotation ) {
    return ( (ShimDependentStep) annotation ).isSeparateClassLoaderNeeded();
  }

  @Override
  protected String extractForumUrl( Annotation annotation ) {
    return ( (ShimDependentStep) annotation ).forumUrl();
  }
}
