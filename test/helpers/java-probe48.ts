/**
 * Redisson 4.8.0 resident probe (interop/pom48.xml → TsProbe48) for the
 * bidirectional live-interop checks of the 4.8-only surfaces: TimeSeries
 * flag-4 members (TimeSeriesAddArgs family), RArray (AR.*) and RCountMin
 * (CMS.*). Same protocol and lazy-Maven behavior as the default 4.6.1 probe;
 * see test/helpers/java-probe.ts.
 */
import { createJavaProbe, type JavaProbe } from './java-probe.js';

export const probe48: JavaProbe = createJavaProbe({
  pom: 'pom48.xml',
  targetDir: 'target48',
  mainClass: 'io.github.redits.interop.TsProbe48',
  sourceFiles: ['src48/main/java/io/github/redits/interop/TsProbe48.java'],
});
