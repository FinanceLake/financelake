# CONTRIBUTING

When contributing to this repository, please first discuss the change you wish to make via [issues](https://github.com/FinanceLake/financelake/issues), [discord](https://discord.com/invite/rP2dNEFJ4Y).

Please note if you are working on a certain issue then make sure to stay active with development.

## Git Commit, Branch, and PR Naming Conventions

When you are working with git, please be sure to follow the conventions below on your pull requests, branches, and commits:

```text
PR: [#ISSUE ID] Title of the PR
Branch: [ISSUE ID]-title-of-the-pr (shorter)
Commit: [[ISSUE ID]] [ACTION]: what was done
```
## Installation

To get started with Code Racer locally, follow these steps

1. Make sure you have installed Docker locally (See above Prerequisites)

2. Fork the repository as your own repo

3. Clone forked repo to your local machine

   ```sh
    git clone https://github.com/<YOUR_GITHUB_ACCOUNT_NAME>/financelake.git
   ```

4. Navigate to the project directory

   ```sh
   cd financelake
   ```

5. Create a .env file inside the project's packages/app directory.

6. Copy and paste variables from `packages/app/.env.example` into `packages/app/.env`

7. Install NPM packages

   ```sh
   npm i
   ```
   
8. Start the Database
    If you are using Docker don't forget to run `docker-compose up` to start the database.
    (Optional if using Docker for managing the database): Start the Database.

   ```sh
   npm run dev:db
   ```

9. Start the app dev server

   ```sh
   npm run dev:app
   ```
10. Start the web socket server

   ```sh
   npm run dev:wss
   ```
Open your browser and visit <http://localhost:3000> to see the application running.

## Working on New Features

If you're new to Github and working with open source repositories, watch this video  which walks you through the process:
[![How to make a pull request on an open source project](https://img.youtube.com/vi/8A4TsoXJOs8/0.jpg)](https://youtu.be/8A4TsoXJOs8)

There is also a  video explaining how you can contribute to open Source  project:
<br/>
[How to contribute to open source projects (our community project walkthrough)](https://www.youtube.com/watch?v=dLRA1lffWBw)

If you want to work on a new feature, follow these steps.

1. Fork the repository
2. Clone your fork
3. Checkout a new branch
4. Do your work
5. Commit
6. Push your branch to your fork
7. Go into github UI and create a PR from your fork & branch, and merge it into upstream MAIN
8. link an issue to pull request
9. Complete check list 

## Pulling in changes from upstream

You should pull in the changes that we add in daily, preferably before you checkout a new branch to do new work.

```sh
git checkout main
```

```sh
git pull upstream main
```

## Before Submitting a Pull Request

Before submitting a **Pull Request**, you should

1. Check your code safety with Linter and TypeScript, and make sure your code can build successfully.

```sh
npm run pr:precheck
```


## Code of Conduct

### Our Pledge

In the interest of fostering an open and welcoming environment, we as
contributors and maintainers pledge to making participation in our project and
our community a friendly experience for everyone, regardless of any experience
to give everyone an opportunity to contribute in this project.

### Our Responsibilities

The primary responsibility of contributors is to provide high-quality code contributions to the project. This involves writing, reviewing, and submitting code changes that improve the project's functionality, fix bugs, or implement new features.

Contributors should actively participate in project discussions and communicate effectively with other contributors, maintainers, and users. This includes joining [discord](https://discord.com/invite/rP2dNEFJ4Y) server solely created for this project.

Contributors can play a role in the long-term maintenance of the project by actively monitoring the project's issue tracker, addressing bug reports and feature requests, and collaborating with other contributors to ensure the project remains healthy and sustainable.

### Scope

This Code of Conduct applies both within project spaces and in public spaces
when an individual is representing the project or its community. Examples of
representing a project or community include using an official project e-mail
address, posting via an official social media account, or acting as an appointed
representative at an online or offline event. Representation of a project may be
further defined and clarified by project maintainers.