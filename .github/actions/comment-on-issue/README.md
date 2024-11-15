# Comment on Issues - GitHub Actions

A GitHub action that comments on issues with a given message.
You can even put dynamic data thanks to [Contexts and expression syntax](https://help.github.com/en/actions/automating-your-workflow-with-github-actions/contexts-and-expression-syntax-for-github-actions).

## Usage

```
name: issue-checklist

on:
  issues:
    types: [opened]

jobs:
  comment:
    runs-on: ubuntu-latest

    steps:
    - uses: ben-z/actions-comment-on-issue@1.0.2
      with:
        message: "Gentle reminder:\n* Did you go through all of the troubleshooting steps outlined in README.md?"
        GITHUB_TOKEN: ${{ secrets.GITHUB_TOKEN }}
```

## Contributing

### Build 

The build steps transpiles the `src/main.ts` to `lib/main.js` which is used in the Docker container. 
It is handled by Typescript compiler. 

```sh
$ npm run build
```
