# Change Log

## 03/01/2022

* feature: upgrade badger to v3 ( #100 )

https://github.com/mimiro-io/datahub/releases/tag/v-0.6.177-stable

## 06/12/2021

* feature: support titles in jobs. ( #88 )
* bugfix: Job get stuck in loop when js transform fails ( #92 )

https://github.com/mimiro-io/datahub/releases/tag/v-0.6.171-stable


## 15/10/2021

* new feature: extended job attributes (#83 )
* new configuration option: value log size (#82 )
* bugfix: continuation tokens of /entities endpoint (#86 )

https://github.com/mimiro-io/datahub/releases/tag/v-0.6.155-stable

## 21/09/2021

* On-start garbage collection (#16)
* BREAKING upgrade badger to v3 (#15, #20, #21)
* Implement fullsync protocol of Universal Data API specification (#14)
* Support parallel processing in javascript transforms (#54)
* Bugfixes (#17, #22, #23, #24, #38, #39, #43)
* Performance improvements (#29, #35, #41)
* Minor improvements (#25, #27, #37, #49, #50, #52)
* More metrics (#11, #39, #44, #47)
* Updated tests, dependencies, documentation (#3, #4, #9, #13, #40, #45, #46, #51)

https://github.com/mimiro-io/datahub/releases/tag/v-0.6.142-stable

## 31/05/2021

* On-start garbage collection (#16)
* BREAKING upgrade badger to v3 (#15, #20, #21)
* Implement fullsync protocol of Universal Data API specification (#14)
* Support parallel processing in javascript transforms (#54)
* Bugfixes (#17, #22, #23, #24, #38, #39, #43 )
* Performance improvements (#29, #35, #41)
* Minor improvements (#25, #27, #37, #49, #50, #52)
* More metrics (#11, #39, #44, #47)
* Updated tests, dependencies, documentation (#3, #4, #9, #13, #40, #45, #46, #51)

https://github.com/mimiro-io/datahub/releases/tag/v-0.6.104-stable

This release of datahub uses a not backwards compatible version of badger. To migrate existing data, the same strategy as
described in https://dgraph.io/blog/post/releasing-badger-v2/#ready-to-migrate can be used.

## 22/03/2021

Initial open source release at version 0.5.X.

