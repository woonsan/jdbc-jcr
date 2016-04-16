How to Release
==============

## Start a new release

        git flow release start RELEASE [BASE]

  For example,

        git flow feature start 1.1 develop

## Publish the release branch

        git flow release publish RELEASE

  For example,

        git flow release publish 1.1

## Finish the release

        git flow release finish RELEASE

  For example,

        git flow release finish 1.1

## Deploy the release

        mvn -Possrh-release deploy

