
cd sdfs/storedfileDir/
mkdir -p $1
cd $1
echo "${2}" > $3
gzip $3
mv $3.gz $3
