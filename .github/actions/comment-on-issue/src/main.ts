const core = require('@actions/core');
const github = require('@actions/github');

async function run() {
  try {
    const message = core.getInput('message');
    const github_token = core.getInput('GITHUB_TOKEN');

    const context = github.context;
    if (context.payload.issue == null) {
        core.setFailed('No issue found.');
        return;
    }
    const issue_number = context.payload.issue.number;

    const octokit = new github.GitHub(github_token);
    const new_comment = octokit.issues.createComment({
        ...context.repo,
        issue_number: issue_number,
        body: message
      });

  } catch (error) {
    core.setFailed(error.message);
  }
}

run();
