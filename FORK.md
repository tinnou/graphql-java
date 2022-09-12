# GraphQL-Java Fork

This is our fork of GraphQL-Java, mainly used in case we need it at this point.

## Changes Made

The mains changes right now are just making sure we're able to build
the library with Nebula / Netflix CI. Here are the current changes made for version `22.x`.

```
PLEASE UPDATE THIS DOC WHEN BUMPING GRAPHQL-JAVA
IF THERE ARE ANY CHANGES.
```

- [ ] Add a `.netflix` folder with `netflix.ci` and `rocket.yml`.
- [ ] Remove agent and agent-test packages. Nebula can't do multi module where on module is at the root apparently.
- [ ] Apply the nebula plugins. It needs to be first apply in the `build.gradle`, so you may have to move `plugins {}` to `apply` instead for some packages.
- [ ] Remove graphql-java publishing and add `nebulaRelease` and friends (See commit for details)
- [ ] Add nebula `distributionUrl` to `gradle-wrapper.properties`
- [ ] Modify `gradlew` (see commit)
- [ ] Add `testImplementation 'org.jetbrains:annotations:23.0.0'`